use sea_query::{Alias, Expr, OnConflict, Order, PostgresQueryBuilder, Query, Value};
use sea_query_binder::SqlxBinder;
use serde::Deserialize;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::types::Uuid;
use sqlx::{PgConnection, Pool, Row, Transaction};

use portfolio::registry::{Chunk, Chunks, ManifestRef, UploadSession, UploadSessions};
use portfolio::{DigestState, OciDigest};

use super::super::errors::{Error, Result};
use super::types::{
    Blob, Blobs, IndexManifests, Layers, Manifest, Manifests, Repositories, Repository, Tag, Tags,
};

#[derive(Clone, Deserialize)]
pub struct PostgresConfig {
    connection_string: String,
}

impl PostgresConfig {
    pub async fn new_metadata(&self) -> Result<PostgresMetadataPool> {
        let pool = PgPoolOptions::new()
            .connect(&self.connection_string)
            .await?;
        Ok(PostgresMetadataPool { pool })
    }
}

#[derive(Clone)]
pub struct PostgresMetadataPool {
    pool: Pool<Postgres>,
}

impl PostgresMetadataPool {
    pub async fn get_conn(&self) -> Result<PostgresMetadataConn> {
        Ok(PostgresMetadataConn {
            conn: self.pool.acquire().await?,
        })
    }

    pub async fn get_tx(&self) -> Result<PostgresMetadataTx> {
        Ok(PostgresMetadataTx {
            tx: Some(self.pool.begin().await?),
        })
    }
}

pub struct PostgresMetadataConn {
    conn: PoolConnection<Postgres>,
}

// A collection of queries that are only require a `&mut PgConnection` and don't care whether it
// came from a transaction or a pool connection.
struct Queries {}

impl Queries {
    pub async fn insert_repository(executor: &mut PgConnection, name: &str) -> Result<Repository> {
        let (sql, values) = Query::insert()
            .into_table(Repositories::Table)
            .columns([Repositories::Name])
            .values([Value::from(name).into()])?
            .returning(Query::returning().columns([Repositories::Id, Repositories::Name]))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Repository, _>(&sql, values)
            .fetch_one(executor)
            .await?)
    }

    pub async fn get_repository(
        executor: &mut PgConnection,
        repository: &str,
    ) -> Result<Option<Repository>> {
        let (sql, values) = Query::select()
            .from(Repositories::Table)
            .columns([
                (Repositories::Table, Repositories::Id),
                (Repositories::Table, Repositories::Name),
            ])
            .and_where(Expr::col((Repositories::Table, Repositories::Name)).eq(repository))
            .build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_as_with::<_, Repository, _>(&sql, values)
            .fetch_optional(executor)
            .await?)
    }

    pub async fn repository_exists(executor: &mut PgConnection, name: &str) -> Result<bool> {
        let (sql, values) = Query::select()
            .expr_as(
                Expr::exists(
                    Query::select()
                        .from(Repositories::Table)
                        .column(Repositories::Id)
                        .and_where(Expr::col(Repositories::Name).eq(name))
                        .to_owned(),
                ),
                Alias::new("exists"),
            )
            .build_sqlx(PostgresQueryBuilder);
        let row = sqlx::query_with(&sql, values).fetch_one(executor).await?;

        Ok(row.try_get("exists")?)
    }
    pub async fn insert_blob(
        executor: &mut PgConnection,
        digest: &OciDigest,
        bytes_on_disk: i64,
    ) -> Result<Uuid> {
        let (sql, values) = Query::insert()
            .into_table(Blobs::Table)
            .columns([Blobs::Digest, Blobs::BytesOnDisk])
            .values([String::from(digest).into(), bytes_on_disk.into()])?
            .returning_col(Blobs::Id)
            .build_sqlx(PostgresQueryBuilder);

        let row = sqlx::query_with(&sql, values).fetch_one(executor).await?;
        Ok(row.try_get("id")?)
    }

    pub async fn get_blob(executor: &mut PgConnection, digest: &OciDigest) -> Result<Option<Blob>> {
        let (sql, values) = Query::select()
            .from(Blobs::Table)
            .columns([Blobs::Id, Blobs::Digest, Blobs::BytesOnDisk])
            // TODO: impl Value for OciDigest
            .and_where(Expr::col(Blobs::Digest).eq(String::from(digest)))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Blob, _>(&sql, values)
            .fetch_optional(executor)
            .await?)
    }

    pub async fn get_blobs(executor: &mut PgConnection, digests: &Vec<&str>) -> Result<Vec<Blob>> {
        let digests = digests.iter().map(Clone::clone);
        let (sql, values) = Query::select()
            .from(Blobs::Table)
            .columns([Blobs::Id, Blobs::Digest, Blobs::BytesOnDisk])
            // TODO: impl Value for OciDigest
            .and_where(Expr::col(Blobs::Digest).is_in(digests))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Blob, _>(&sql, values)
            .fetch_all(executor)
            .await?)
    }

    pub async fn delete_blob(executor: &mut PgConnection, blob_id: &Uuid) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(Blobs::Table)
            .cond_where(Expr::col(Blobs::Id).eq(*blob_id))
            .build_sqlx(PostgresQueryBuilder);
        match sqlx::query_with(&sql, values).execute(executor).await {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(dberr)) => match dberr.kind() {
                sqlx::error::ErrorKind::ForeignKeyViolation => {
                    tracing::warn!("foreign key violation error: {dberr}");
                    Err(Error::DistributionSpecError(
                        portfolio::DistributionErrorCode::ContentReferenced,
                    ))
                }
                _ => Err(sqlx::Error::Database(dberr).into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_manifests(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        digests: &Vec<&str>,
    ) -> Result<Vec<Manifest>> {
        let digests = digests.iter().map(Clone::clone);
        let (sql, values) = Query::select()
            .from(Manifests::Table)
            .columns([
                (Manifests::Table, Manifests::Id),
                (Manifests::Table, Manifests::RepositoryId),
                (Manifests::Table, Manifests::BlobId),
                (Manifests::Table, Manifests::MediaType),
                (Manifests::Table, Manifests::ArtifactType),
                (Manifests::Table, Manifests::Digest),
                (Manifests::Table, Manifests::Subject),
            ])
            .column((Blobs::Table, Blobs::BytesOnDisk))
            .left_join(
                Blobs::Table,
                Expr::col((Manifests::Table, Manifests::BlobId)).equals((Blobs::Table, Blobs::Id)),
            )
            .and_where(Expr::col((Manifests::Table, Manifests::RepositoryId)).eq(*repository_id))
            .and_where(Expr::col((Manifests::Table, Manifests::Digest)).is_in(digests))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Manifest, _>(&sql, values)
            .fetch_all(executor)
            .await?)
    }

    pub async fn get_manifest(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        manifest_ref: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        let mut builder = Query::select();
        builder
            .from(Manifests::Table)
            .columns([
                (Manifests::Table, Manifests::Id),
                (Manifests::Table, Manifests::RepositoryId),
                (Manifests::Table, Manifests::BlobId),
                (Manifests::Table, Manifests::MediaType),
                (Manifests::Table, Manifests::ArtifactType),
                (Manifests::Table, Manifests::Digest),
                (Manifests::Table, Manifests::Subject),
            ])
            .column((Blobs::Table, Blobs::BytesOnDisk))
            .left_join(
                Blobs::Table,
                Expr::col((Manifests::Table, Manifests::BlobId)).equals((Blobs::Table, Blobs::Id)),
            )
            .and_where(Expr::col((Manifests::Table, Manifests::RepositoryId)).eq(*repository_id));

        match manifest_ref {
            ManifestRef::Digest(d) => {
                builder.and_where(
                    Expr::col((Manifests::Table, Manifests::Digest)).eq(String::from(d)),
                );
            }
            ManifestRef::Tag(t) => {
                builder
                    .left_join(
                        Tags::Table,
                        Expr::col((Tags::Table, Tags::ManifestId))
                            .equals((Manifests::Table, Manifests::Id)),
                    )
                    .and_where(Expr::col((Tags::Table, Tags::Name)).eq(t));
            }
        }

        let (sql, values) = builder.build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_as_with::<_, Manifest, _>(&sql, values)
            .fetch_optional(executor)
            .await?)
    }

    pub async fn insert_manifest(executor: &mut PgConnection, manifest: &Manifest) -> Result<()> {
        let (sql, values) = Query::insert()
            .into_table(Manifests::Table)
            .columns([
                Manifests::Id,
                Manifests::RepositoryId,
                Manifests::BlobId,
                Manifests::MediaType,
                Manifests::ArtifactType,
                Manifests::Digest,
                Manifests::Subject,
            ])
            .values([
                Value::from(manifest.id).into(),
                Value::from(manifest.repository_id).into(),
                Value::from(manifest.blob_id).into(),
                Value::from(manifest.media_type.clone().map(String::from)).into(),
                Value::from(manifest.artifact_type.clone().map(String::from)).into(),
                Value::from(String::from(&manifest.digest)).into(),
                Value::from(manifest.subject.clone().map(String::from)).into(),
            ])?
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn delete_manifest(executor: &mut PgConnection, manifest_id: &Uuid) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(Manifests::Table)
            .cond_where(Expr::col(Manifests::Id).eq(*manifest_id))
            .build_sqlx(PostgresQueryBuilder);

        match sqlx::query_with(&sql, values).execute(executor).await {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(dberr)) => match dberr.kind() {
                sqlx::error::ErrorKind::ForeignKeyViolation => {
                    tracing::warn!("foreign key violation error: {dberr}");
                    Err(Error::DistributionSpecError(
                        portfolio::DistributionErrorCode::ContentReferenced,
                    ))
                }
                _ => Err(sqlx::Error::Database(dberr).into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    pub async fn associate_image_layers(
        executor: &mut PgConnection,
        parent: &Uuid,
        children: Vec<&Uuid>,
    ) -> Result<()> {
        let mut builder = Query::insert();
        builder
            .into_table(Layers::Table)
            .columns([Layers::Manifest, Layers::Blob]);

        for child in children.iter() {
            builder.values([
                Value::from(parent.clone()).into(),
                Value::from((*child).clone()).into(),
            ])?;
        }

        let (sql, values) = builder.build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values).execute(executor).await?;

        Ok(())
    }

    pub async fn delete_image_layers(executor: &mut PgConnection, parent: &Uuid) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(Layers::Table)
            .cond_where(Expr::col(Layers::Manifest).eq(*parent))
            .build_sqlx(PostgresQueryBuilder);

        match sqlx::query_with(&sql, values).execute(executor).await {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(dberr)) => match dberr.kind() {
                sqlx::error::ErrorKind::ForeignKeyViolation => {
                    tracing::warn!("foreign key violation error: {dberr}");
                    Err(Error::DistributionSpecError(
                        portfolio::DistributionErrorCode::ContentReferenced,
                    ))
                }
                _ => Err(sqlx::Error::Database(dberr).into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    pub async fn associate_index_manifests(
        executor: &mut PgConnection,
        parent: &Uuid,
        children: Vec<&Uuid>,
    ) -> Result<()> {
        let mut builder = Query::insert();
        builder.into_table(IndexManifests::Table).columns([
            IndexManifests::ParentManifest,
            IndexManifests::ChildManifest,
        ]);

        for child in children.iter() {
            builder.values([
                Value::from(parent.clone()).into(),
                Value::from((*child).clone()).into(),
            ])?;
        }

        let (sql, values) = builder.build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn delete_index_manifests(executor: &mut PgConnection, parent: &Uuid) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(IndexManifests::Table)
            .cond_where(Expr::col(IndexManifests::ParentManifest).eq(*parent))
            .build_sqlx(PostgresQueryBuilder);

        match sqlx::query_with(&sql, values).execute(executor).await {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(dberr)) => match dberr.kind() {
                sqlx::error::ErrorKind::ForeignKeyViolation => {
                    tracing::warn!("foreign key violation error: {dberr}");
                    Err(Error::DistributionSpecError(
                        portfolio::DistributionErrorCode::ContentReferenced,
                    ))
                }
                _ => Err(sqlx::Error::Database(dberr).into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    pub async fn upsert_tag(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        manifest_id: &Uuid,
        tag: &str,
    ) -> Result<()> {
        let (sql, values) = Query::insert()
            .into_table(Tags::Table)
            .columns([Tags::Name, Tags::RepositoryId, Tags::ManifestId])
            .values([
                Value::from(tag).into(),
                Value::from(*repository_id).into(),
                Value::from(*manifest_id).into(),
            ])?
            .on_conflict(
                OnConflict::columns([Tags::RepositoryId, Tags::Name])
                    .update_columns([Tags::ManifestId])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn get_tags(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        n: Option<i64>,
        last: Option<String>,
    ) -> Result<Vec<Tag>> {
        let mut builder = Query::select();
        builder
            .columns([Tags::ManifestId, Tags::Name])
            .column((Manifests::Table, Manifests::Digest))
            .left_join(
                Manifests::Table,
                Expr::col((Tags::Table, Tags::ManifestId))
                    .equals((Manifests::Table, Manifests::Id)),
            )
            .from(Tags::Table)
            .and_where(Expr::col((Tags::Table, Tags::RepositoryId)).eq(*repository_id));

        match (n, last) {
            (Some(n), Some(last)) => {
                builder
                    .and_where(
                        Expr::tuple([
                            Expr::col((Tags::Table, Tags::RepositoryId)).into(),
                            Expr::col(Tags::Name).into(),
                        ])
                        .gt(Expr::tuple([
                            Expr::value(*repository_id),
                            Expr::value(last),
                        ])),
                    )
                    .limit(n as u64);
            }
            (Some(n), None) => {
                builder.limit(n as u64);
            }
            (None, Some(_)) => return Err(Error::MissingQueryParameter("n")),
            (None, None) => {}
        }
        builder.order_by_columns(vec![
            ((Tags::Table, Tags::Name), Order::Asc),
            ((Tags::Table, Tags::RepositoryId), Order::Asc),
        ]);

        let (sql, values) = builder.build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_as_with::<_, Tag, _>(&sql, values)
            .fetch_all(executor)
            .await?)
    }

    pub async fn delete_tags_by_manifest_id(
        executor: &mut PgConnection,
        manifest_id: &Uuid,
    ) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(Tags::Table)
            .cond_where(Expr::col(Tags::ManifestId).eq(*manifest_id))
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn get_chunks(
        executor: &mut PgConnection,
        session: &UploadSession,
    ) -> Result<Vec<Chunk>> {
        let (sql, values) = Query::select()
            .from(Chunks::Table)
            .columns([Chunks::ETag, Chunks::ChunkNumber])
            .and_where(Expr::col(Chunks::UploadSessionUuid).eq(session.uuid))
            .order_by(Chunks::ChunkNumber, Order::Asc)
            .build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_as_with::<_, Chunk, _>(&sql, values)
            .fetch_all(executor)
            .await?)
    }

    pub async fn insert_chunk(
        executor: &mut PgConnection,
        session: &UploadSession,
        chunk: &Chunk,
    ) -> Result<()> {
        let (sql, values) = Query::insert()
            .into_table(Chunks::Table)
            .columns([Chunks::ChunkNumber, Chunks::UploadSessionUuid, Chunks::ETag])
            .values([
                Value::from(chunk.chunk_number).into(),
                Value::from(session.uuid).into(),
                Value::from(chunk.e_tag.clone()).into(),
            ])?
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn delete_chunks(executor: &mut PgConnection, uuid: &Uuid) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(Chunks::Table)
            .and_where(Expr::col(Chunks::UploadSessionUuid).eq(*uuid))
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn new_upload_session(executor: &mut PgConnection) -> Result<UploadSession> {
        let state = DigestState::default();
        let value = serde_json::value::to_value(state)?;
        let (sql, values) = Query::insert()
            .into_table(UploadSessions::Table)
            .columns([UploadSessions::DigestState])
            .values([Expr::value(value)])?
            .returning(Query::returning().columns([
                UploadSessions::Uuid,
                UploadSessions::StartDate,
                UploadSessions::UploadId,
                UploadSessions::ChunkNumber,
                UploadSessions::LastRangeEnd,
                UploadSessions::DigestState,
            ]))
            .build_sqlx(PostgresQueryBuilder);
        let session = sqlx::query_as_with::<_, UploadSession, _>(&sql, values)
            .fetch_one(executor)
            .await?;

        Ok(session)
    }

    pub async fn get_session(executor: &mut PgConnection, uuid: &Uuid) -> Result<UploadSession> {
        let (sql, values) = Query::select()
            .from(UploadSessions::Table)
            .columns([
                UploadSessions::Uuid,
                UploadSessions::StartDate,
                UploadSessions::ChunkNumber,
                UploadSessions::LastRangeEnd,
                UploadSessions::UploadId,
                UploadSessions::DigestState,
            ])
            .and_where(Expr::col(UploadSessions::Uuid).eq(*uuid))
            .build_sqlx(PostgresQueryBuilder);
        let session = sqlx::query_as_with::<_, UploadSession, _>(&sql, values)
            .fetch_one(executor)
            .await?;

        Ok(session)
    }

    pub async fn update_session(
        executor: &mut PgConnection,
        session: &UploadSession,
    ) -> Result<()> {
        let state = serde_json::value::to_value(&session.digest_state)?;
        let (sql, values) = Query::update()
            .table(UploadSessions::Table)
            .and_where(Expr::col(UploadSessions::Uuid).eq(session.uuid))
            .value(UploadSessions::UploadId, session.upload_id.clone())
            .value(UploadSessions::ChunkNumber, session.chunk_number)
            .value(UploadSessions::LastRangeEnd, session.last_range_end)
            .value(UploadSessions::DigestState, state)
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn delete_session(
        executor: &mut PgConnection,
        session: &UploadSession,
    ) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(UploadSessions::Table)
            .and_where(Expr::col(UploadSessions::Uuid).eq(session.uuid))
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values).execute(executor).await?;
        Ok(())
    }

    pub async fn get_referrers(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        subject: &OciDigest,
        artifact_type: &Option<String>,
    ) -> Result<Vec<Manifest>> {
        let mut builder = Query::select();
        builder
            .from(Manifests::Table)
            .columns([
                (Manifests::Table, Manifests::Id),
                (Manifests::Table, Manifests::RepositoryId),
                (Manifests::Table, Manifests::BlobId),
                (Manifests::Table, Manifests::MediaType),
                (Manifests::Table, Manifests::ArtifactType),
                (Manifests::Table, Manifests::Digest),
                (Manifests::Table, Manifests::Subject),
            ])
            .column((Blobs::Table, Blobs::BytesOnDisk))
            .left_join(
                Blobs::Table,
                Expr::col((Manifests::Table, Manifests::BlobId)).equals((Blobs::Table, Blobs::Id)),
            )
            .order_by(Manifests::Digest, Order::Asc)
            .and_where(Expr::col((Manifests::Table, Manifests::RepositoryId)).eq(*repository_id))
            .and_where(Expr::col((Manifests::Table, Manifests::Subject)).eq(String::from(subject)));

        if let Some(artifact_type) = artifact_type {
            builder.and_where(
                Expr::col((Manifests::Table, Manifests::ArtifactType)).eq(artifact_type),
            );
        }

        let (sql, values) = builder.build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_as_with::<_, Manifest, _>(&sql, values)
            .fetch_all(executor)
            .await?)
    }
}

// PoolConnection<Postgres>-based metadata queries.
impl PostgresMetadataConn {
    pub async fn insert_repository(&mut self, name: &str) -> Result<Repository> {
        Queries::insert_repository(&mut *self.conn, name).await
    }

    pub async fn get_repository(&mut self, repository: &str) -> Result<Option<Repository>> {
        Queries::get_repository(&mut *self.conn, repository).await
    }

    pub async fn repository_exists(&mut self, name: &str) -> Result<bool> {
        Queries::repository_exists(&mut *self.conn, name).await
    }

    pub async fn insert_blob(&mut self, digest: &OciDigest, bytes_on_disk: i64) -> Result<Uuid> {
        Queries::insert_blob(&mut *self.conn, digest, bytes_on_disk).await
    }

    pub async fn get_blob(&mut self, digest: &OciDigest) -> Result<Option<Blob>> {
        Queries::get_blob(&mut *self.conn, digest).await
    }

    pub async fn get_manifest(
        &mut self,
        repository_id: &Uuid,
        manifest_ref: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        Queries::get_manifest(&mut *self.conn, repository_id, manifest_ref).await
    }

    pub async fn get_tags(
        &mut self,
        repository_id: &Uuid,
        n: Option<i64>,
        last: Option<String>,
    ) -> Result<Vec<Tag>> {
        Queries::get_tags(&mut *self.conn, repository_id, n, last).await
    }

    pub async fn new_upload_session(&mut self) -> Result<UploadSession> {
        Queries::new_upload_session(&mut *self.conn).await
    }

    pub async fn get_session(&mut self, uuid: &Uuid) -> Result<UploadSession> {
        Queries::get_session(&mut *self.conn, uuid).await
    }

    pub async fn update_session(&mut self, session: &UploadSession) -> Result<()> {
        Queries::update_session(&mut *self.conn, session).await
    }

    pub async fn delete_chunks(&mut self, uuid: &Uuid) -> Result<()> {
        Queries::delete_chunks(&mut *self.conn, uuid).await
    }

    pub async fn delete_session(&mut self, session: &UploadSession) -> Result<()> {
        Queries::delete_session(&mut *self.conn, session).await
    }

    pub async fn get_chunks(&mut self, session: &UploadSession) -> Result<Vec<Chunk>> {
        Queries::get_chunks(&mut *self.conn, session).await
    }

    pub async fn insert_chunk(&mut self, session: &UploadSession, chunk: &Chunk) -> Result<()> {
        Queries::insert_chunk(&mut *self.conn, session, chunk).await
    }

    pub async fn get_referrers(
        &mut self,
        repository_id: &Uuid,
        subject: &OciDigest,
        artifact_type: &Option<String>,
    ) -> Result<Vec<Manifest>> {
        Queries::get_referrers(&mut *self.conn, repository_id, subject, artifact_type).await
    }
}

// Wrapper around a Postgres transaction with the ability to commit transactions.
pub struct PostgresMetadataTx<'a> {
    tx: Option<Transaction<'a, Postgres>>,
}

impl<'a> PostgresMetadataTx<'a> {
    pub async fn commit(&mut self) -> Result<()> {
        if let Some(t) = self.tx.take() {
            Ok(t.commit().await?)
        } else {
            Ok(())
        }
    }

    pub async fn insert_blob(&mut self, digest: &OciDigest, bytes_on_disk: i64) -> Result<Uuid> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::insert_blob(&mut **tx, digest, bytes_on_disk).await
    }

    pub async fn insert_chunk(&mut self, session: &UploadSession, chunk: &Chunk) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::insert_chunk(&mut **tx, session, chunk).await
    }

    pub async fn get_chunks(&mut self, session: &UploadSession) -> Result<Vec<Chunk>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_chunks(&mut **tx, session).await
    }

    pub async fn delete_chunks(&mut self, uuid: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_chunks(&mut **tx, uuid).await
    }

    pub async fn update_session(&mut self, session: &UploadSession) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::update_session(&mut **tx, session).await
    }

    pub async fn delete_session(&mut self, session: &UploadSession) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_session(&mut **tx, session).await
    }

    pub async fn get_blob(&mut self, digest: &OciDigest) -> Result<Option<Blob>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_blob(&mut **tx, digest).await
    }

    pub async fn get_blobs(&mut self, digests: &Vec<&str>) -> Result<Vec<Blob>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_blobs(&mut **tx, digests).await
    }

    pub async fn delete_blob(&mut self, blob_id: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_blob(&mut **tx, blob_id).await
    }

    pub async fn get_manifests(
        &mut self,
        repository_id: &Uuid,
        digests: &Vec<&str>,
    ) -> Result<Vec<Manifest>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_manifests(&mut **tx, repository_id, digests).await
    }

    pub async fn get_manifest(
        &mut self,
        repository_id: &Uuid,
        reference: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_manifest(&mut **tx, repository_id, reference).await
    }

    pub async fn insert_manifest(&mut self, manifest: &Manifest) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::insert_manifest(&mut **tx, manifest).await
    }

    pub async fn delete_manifest(&mut self, manifest_id: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_manifest(&mut **tx, manifest_id).await
    }

    pub async fn associate_image_layers(
        &mut self,
        parent: &Uuid,
        children: Vec<&Uuid>,
    ) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::associate_image_layers(&mut **tx, parent, children).await
    }

    pub async fn delete_image_layers(&mut self, parent: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_image_layers(&mut **tx, parent).await
    }

    pub async fn associate_index_manifests(
        &mut self,
        parent: &Uuid,
        children: Vec<&Uuid>,
    ) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::associate_index_manifests(&mut **tx, parent, children).await
    }

    pub async fn delete_index_manifests(&mut self, parent: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_index_manifests(&mut **tx, parent).await
    }

    pub async fn upsert_tag(
        &mut self,
        repository_id: &Uuid,
        manifest_id: &Uuid,
        tag: &str,
    ) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::upsert_tag(&mut **tx, repository_id, manifest_id, tag).await
    }

    pub async fn delete_tags_by_manifest_id(&mut self, manifest_id: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_tags_by_manifest_id(&mut **tx, manifest_id).await
    }
}
