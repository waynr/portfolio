use sea_query::{Alias, Expr, OnConflict, Order, PostgresQueryBuilder, Query, Value};
use sea_query_binder::SqlxBinder;
use serde::Deserialize;
use sqlx::{
    pool::PoolConnection,
    postgres::{PgPoolOptions, Postgres},
    types::Uuid,
    PgConnection, Pool, Row, Transaction,
};

use crate::errors::{Error, Result};
use crate::metadata::{
    Blob, Blobs, IndexManifests, Layers, Manifest, ManifestRef, Manifests, Registries, Registry,
    Repositories, Repository, Tag, Tags,
};
use crate::registry::{Chunk, Chunks, UploadSession, UploadSessions};
use crate::OciDigest;
use crate::{DigestState, RegistryDefinition};

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
    pub async fn insert_blob(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Uuid> {
        let (sql, values) = Query::insert()
            .into_table(Blobs::Table)
            .columns([Blobs::Digest, Blobs::RegistryId])
            .values([String::from(digest).into(), (*registry_id).into()])?
            .returning_col(Blobs::Id)
            .build_sqlx(PostgresQueryBuilder);

        let row = sqlx::query_with(&sql, values).fetch_one(executor).await?;
        Ok(row.try_get("id")?)
    }

    pub async fn get_blob(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Option<Blob>> {
        let (sql, values) = Query::select()
            .from(Blobs::Table)
            .columns([Blobs::Id, Blobs::Digest, Blobs::RegistryId])
            .and_where(Expr::col(Blobs::RegistryId).eq(*registry_id))
            // TODO: impl Value for OciDigest
            .and_where(Expr::col(Blobs::Digest).eq(String::from(digest)))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Blob, _>(&sql, values)
            .fetch_optional(executor)
            .await?)
    }

    pub async fn get_blobs(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        digests: &Vec<&str>,
    ) -> Result<Vec<Blob>> {
        let digests = digests.iter().map(Clone::clone);
        let (sql, values) = Query::select()
            .from(Blobs::Table)
            .columns([Blobs::Id, Blobs::Digest, Blobs::RegistryId])
            .and_where(Expr::col(Blobs::RegistryId).eq(*registry_id))
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
                        crate::DistributionErrorCode::ContentReferenced,
                    ))
                }
                _ => Err(sqlx::Error::Database(dberr).into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_manifests(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        repository_id: &Uuid,
        digests: &Vec<&str>,
    ) -> Result<Vec<Manifest>> {
        let digests = digests.iter().map(Clone::clone);
        let (sql, values) = Query::select()
            .from(Manifests::Table)
            .columns([
                Manifests::Id,
                Manifests::RegistryId,
                Manifests::RepositoryId,
                Manifests::BlobId,
                Manifests::MediaType,
                Manifests::ArtifactType,
                Manifests::Digest,
            ])
            .and_where(Expr::col(Manifests::RepositoryId).eq(*repository_id))
            .and_where(Expr::col(Manifests::RegistryId).eq(*registry_id))
            .and_where(Expr::col(Manifests::Digest).is_in(digests))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Manifest, _>(&sql, values)
            .fetch_all(executor)
            .await?)
    }

    pub async fn get_manifest(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        repository_id: &Uuid,
        manifest_ref: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        let mut builder = Query::select();
        builder
            .from(Manifests::Table)
            .columns([
                (Manifests::Table, Manifests::Id),
                (Manifests::Table, Manifests::RegistryId),
                (Manifests::Table, Manifests::RepositoryId),
                (Manifests::Table, Manifests::BlobId),
                (Manifests::Table, Manifests::MediaType),
                (Manifests::Table, Manifests::ArtifactType),
                (Manifests::Table, Manifests::Digest),
            ])
            .and_where(Expr::col((Manifests::Table, Manifests::RepositoryId)).eq(*repository_id))
            .and_where(Expr::col(Manifests::RegistryId).eq(*registry_id));

        match manifest_ref {
            ManifestRef::Digest(d) => {
                builder.and_where(Expr::col(Manifests::Digest).eq(String::from(d)));
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
                Manifests::RegistryId,
                Manifests::RepositoryId,
                Manifests::BlobId,
                Manifests::MediaType,
                Manifests::ArtifactType,
                Manifests::Digest,
            ])
            .values([
                Value::from(manifest.id).into(),
                Value::from(manifest.registry_id).into(),
                Value::from(manifest.repository_id).into(),
                Value::from(manifest.blob_id).into(),
                Value::from(manifest.media_type.clone().map(String::from)).into(),
                Value::from(manifest.artifact_type.clone().map(String::from)).into(),
                Value::from(String::from(&manifest.digest)).into(),
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
                        crate::DistributionErrorCode::ContentReferenced,
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
                        crate::DistributionErrorCode::ContentReferenced,
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
                        crate::DistributionErrorCode::ContentReferenced,
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

    pub async fn delete_chunks(executor: &mut PgConnection, uuid: &Uuid) -> Result<()> {
        let (sql, values) = Query::delete()
            .from_table(Chunks::Table)
            .and_where(Expr::col(Chunks::UploadSessionUuid).eq(*uuid))
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
}

// PoolConnection<Postgres>-based metadata queries.
impl PostgresMetadataConn {
    pub async fn insert_registry(&mut self, name: &String) -> Result<Registry> {
        let (sql, values) = Query::insert()
            .into_table(Registries::Table)
            .columns([Registries::Name])
            .values([name.into()])?
            .returning(Query::returning().columns([Registries::Id, Registries::Name]))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Registry, _>(&sql, values)
            .fetch_one(&mut *self.conn)
            .await?)
    }

    pub async fn get_registry(&mut self, name: &str) -> Result<Registry> {
        let (sql, values) = Query::select()
            .from(Registries::Table)
            .columns([Registries::Id, Registries::Name])
            .and_where(Expr::col(Registries::Name).eq(name))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Registry, _>(&sql, values)
            .fetch_one(&mut *self.conn)
            .await?)
    }

    pub async fn insert_repository(
        &mut self,
        registry_id: &Uuid,
        name: &String,
    ) -> Result<Repository> {
        let (sql, values) = Query::insert()
            .into_table(Repositories::Table)
            .columns([Repositories::Name, Repositories::RegistryId])
            .values([Value::from(name).into(), Value::from(*registry_id).into()])?
            .returning(Query::returning().columns([
                Repositories::Id,
                Repositories::Name,
                Repositories::RegistryId,
            ]))
            .build_sqlx(PostgresQueryBuilder);

        Ok(sqlx::query_as_with::<_, Repository, _>(&sql, values)
            .fetch_one(&mut *self.conn)
            .await?)
    }

    pub async fn get_repository(
        &mut self,
        registry: &Uuid,
        repository: &String,
    ) -> Result<Repository> {
        let (sql, values) = Query::select()
            .from(Repositories::Table)
            .columns([
                (Repositories::Table, Repositories::Id),
                (Repositories::Table, Repositories::Name),
                (Repositories::Table, Repositories::RegistryId),
            ])
            .left_join(
                Registries::Table,
                Expr::col((Registries::Table, Registries::Id)).equals(Repositories::RegistryId),
            )
            .and_where(Expr::col((Registries::Table, Registries::Id)).eq(*registry))
            .and_where(Expr::col((Repositories::Table, Repositories::Name)).eq(repository))
            .build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_as_with::<_, Repository, _>(&sql, values)
            .fetch_one(&mut *self.conn)
            .await?)
    }

    pub async fn insert_blob(&mut self, registry_id: &Uuid, digest: &OciDigest) -> Result<Uuid> {
        Queries::insert_blob(&mut *self.conn, registry_id, digest).await
    }

    pub async fn get_blob(
        &mut self,
        registry_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Option<Blob>> {
        Queries::get_blob(&mut *self.conn, registry_id, digest).await
    }

    pub async fn repository_exists(&mut self, registry_id: &Uuid, name: &String) -> Result<bool> {
        let (sql, values) = Query::select()
            .expr_as(
                Expr::exists(
                    Query::select()
                        .from(Repositories::Table)
                        .column(Repositories::Id)
                        .and_where(Expr::col(Repositories::RegistryId).eq(*registry_id))
                        .and_where(Expr::col(Repositories::Name).eq(name))
                        .to_owned(),
                ),
                Alias::new("exists"),
            )
            .build_sqlx(PostgresQueryBuilder);
        let row = sqlx::query_with(&sql, values)
            .fetch_one(&mut *self.conn)
            .await?;

        Ok(row.try_get("exists")?)
    }

    pub async fn blob_exists(&mut self, registry_id: &Uuid, digest: &OciDigest) -> Result<bool> {
        let (sql, values) = Query::select()
            .expr_as(
                Expr::exists(
                    Query::select()
                        .from(Blobs::Table)
                        .column(Blobs::Id)
                        .and_where(Expr::col(Blobs::RegistryId).eq(*registry_id))
                        .and_where(Expr::col(Blobs::Digest).eq(String::from(digest).as_str()))
                        .to_owned(),
                ),
                Alias::new("exists"),
            )
            .build_sqlx(PostgresQueryBuilder);
        let row = sqlx::query_with(&sql, values)
            .fetch_one(&mut *self.conn)
            .await?;

        Ok(row.try_get("exists")?)
    }

    pub async fn get_manifest(
        &mut self,
        registry_id: &Uuid,
        repository_id: &Uuid,
        manifest_ref: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        Queries::get_manifest(&mut *self.conn, registry_id, repository_id, manifest_ref).await
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
            .fetch_one(&mut *self.conn)
            .await?;

        Ok(session)
    }

    pub async fn get_session(&mut self, uuid: &Uuid) -> Result<UploadSession> {
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
            .fetch_one(&mut *self.conn)
            .await?;

        Ok(session)
    }

    pub async fn update_session(&mut self, session: &UploadSession) -> Result<()> {
        let state = serde_json::value::to_value(&session.digest_state)?;
        let (sql, values) = Query::update()
            .table(UploadSessions::Table)
            .and_where(Expr::col(UploadSessions::Uuid).eq(session.uuid))
            .value(UploadSessions::UploadId, session.upload_id.clone())
            .value(UploadSessions::ChunkNumber, session.chunk_number)
            .value(UploadSessions::LastRangeEnd, session.last_range_end)
            .value(UploadSessions::DigestState, state)
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(&mut *self.conn)
            .await?;
        Ok(())
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
        let (sql, values) = Query::insert()
            .into_table(Chunks::Table)
            .columns([Chunks::ChunkNumber, Chunks::UploadSessionUuid, Chunks::ETag])
            .values([
                Value::from(chunk.chunk_number).into(),
                Value::from(session.uuid).into(),
                Value::from(chunk.e_tag.clone()).into(),
            ])?
            .build_sqlx(PostgresQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(&mut *self.conn)
            .await?;
        Ok(())
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

    pub async fn insert_blob(&mut self, registry_id: &Uuid, digest: &OciDigest) -> Result<Uuid> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::insert_blob(&mut **tx, registry_id, digest).await
    }

    pub async fn get_chunks(&mut self, session: &UploadSession) -> Result<Vec<Chunk>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_chunks(&mut **tx, session).await
    }

    pub async fn delete_chunks(&mut self, uuid: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_chunks(&mut **tx, uuid).await
    }

    pub async fn delete_session(&mut self, session: &UploadSession) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_session(&mut **tx, session).await
    }

    pub async fn get_blob(
        &mut self,
        registry_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Option<Blob>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_blob(&mut **tx, registry_id, digest).await
    }

    pub async fn get_blobs(
        &mut self,
        registry_id: &Uuid,
        digests: &Vec<&str>,
    ) -> Result<Vec<Blob>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_blobs(&mut **tx, registry_id, digests).await
    }

    pub async fn delete_blob(&mut self, blob_id: &Uuid) -> Result<()> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::delete_blob(&mut **tx, blob_id).await
    }

    pub async fn get_manifests(
        &mut self,
        registry_id: &Uuid,
        repository_id: &Uuid,
        digests: &Vec<&str>,
    ) -> Result<Vec<Manifest>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_manifests(&mut **tx, registry_id, repository_id, digests).await
    }

    pub async fn get_manifest(
        &mut self,
        registry_id: &Uuid,
        repository_id: &Uuid,
        reference: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_manifest(&mut **tx, registry_id, repository_id, reference).await
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

// higher level DB interaction methods
impl PostgresMetadataConn {
    pub async fn initialize_static_registries(
        &mut self,
        registries: Vec<RegistryDefinition>,
    ) -> Result<()> {
        for registry_config in registries {
            let registry = match self.get_registry(&registry_config.name).await {
                Ok(r) => r,
                Err(Error::SQLXError(sqlx::Error::RowNotFound)) => {
                    tracing::info!(
                        "static registry '{}' not found, inserting into DB",
                        registry_config.name
                    );
                    self.insert_registry(&registry_config.name).await?
                }
                Err(e) => return Err(e),
            };

            for repository_config in registry_config.repositories {
                match self
                    .get_repository(&registry.id, &repository_config.name)
                    .await
                {
                    Ok(r) => r,
                    Err(Error::SQLXError(sqlx::Error::RowNotFound)) => {
                        tracing::info!(
                            "static repository '{}' for registry '{}' not found, inserting into DB",
                            repository_config.name,
                            registry_config.name
                        );
                        self.insert_repository(&registry.id, &repository_config.name)
                            .await?
                    }
                    Err(e) => return Err(e),
                };
            }
        }
        Ok(())
    }
}
