use serde::Deserialize;
use sqlx::{
    pool::PoolConnection,
    postgres::{PgPoolOptions, Postgres},
    types::{Json, Uuid},
    Execute, PgConnection, Pool, QueryBuilder, Transaction,
};
use tokio_stream::StreamExt;

use crate::errors::{Error, Result};
use crate::metadata::{Blob, Manifest, ManifestRef, Registry, Repository, Tag};
use crate::registry::{Chunk, UploadSession};
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
        let record = sqlx::query!(
            r#"
INSERT INTO blobs ( digest, registry_id )
VALUES ( $1, $2 )
RETURNING id
            "#,
            String::from(digest),
            registry_id,
        )
        .fetch_one(executor)
        .await?;

        Ok(record.id)
    }

    pub async fn get_blob(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Option<Blob>> {
        let row = sqlx::query!(
            r#"
SELECT id, digest, registry_id
FROM blobs
WHERE registry_id = $1 AND digest = $2
            "#,
            registry_id,
            String::from(digest),
        )
        .fetch_optional(executor)
        .await?;
        if let Some(row) = row {
            return Ok(Some(Blob {
                id: row.id,
                registry_id: row.registry_id,
                digest: row.digest.as_str().try_into()?,
            }));
        }
        Ok(None)
    }

    pub async fn get_blobs(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        digests: &Vec<&str>,
    ) -> Result<Vec<Blob>> {
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT b.id, b.digest, b.registry_id FROM blobs b WHERE (b.registry_id = ",
        );
        qb.push_bind(registry_id);
        qb.push(") AND b.digest IN (");
        let mut separated = qb.separated(", ");
        for dgst in digests {
            separated.push_bind(dgst);
        }
        separated.push_unseparated(") ");
        let query = qb.build_query_as::<Blob>();
        tracing::debug!("get_tags sql: {}", query.sql());
        Ok(query.fetch_all(executor).await?)
    }

    pub async fn delete_blob(executor: &mut PgConnection, blob_id: &Uuid) -> Result<()> {
        match sqlx::query!(
            r#"
DELETE FROM blobs
WHERE id = $1
            "#,
            blob_id
        )
        .execute(executor)
        .await
        {
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
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
SELECT m.id, m.registry_id, m.repository_id, m.blob_id, m.media_type, m.artifact_type, m.digest
FROM manifests m 
WHERE m.registry_id = "#,
        );
        qb.push_bind(registry_id);
        qb.push(" AND m.repository_id = ");
        qb.push_bind(repository_id);
        qb.push(" AND m.digest IN (");

        let mut separated = qb.separated(", ");
        for dgst in digests {
            separated.push_bind(dgst);
        }
        separated.push_unseparated(") ");
        let query = qb.build_query_as::<Manifest>();
        tracing::debug!("get_manifests sql: {}", query.sql());
        Ok(query.fetch_all(executor).await?)
    }

    pub async fn get_manifest(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        repository_id: &Uuid,
        manifest_ref: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        let manifest = match manifest_ref {
            ManifestRef::Digest(d) => {
                Self::get_manifest_by_digest(executor, registry_id, repository_id, &d).await?
            }
            ManifestRef::Tag(s) => {
                Self::get_manifest_by_tag(executor, registry_id, repository_id, &s).await?
            }
        };

        Ok(manifest)
    }
    pub async fn get_manifest_by_digest(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        repository_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Option<Manifest>> {
        let row = sqlx::query!(
            r#"
SELECT m.id, m.registry_id, m.repository_id, m.blob_id, m.media_type, m.artifact_type, m.digest
FROM manifests m 
WHERE m.registry_id = $1 AND m.repository_id = $2 AND m.digest = $3
            "#,
            registry_id,
            repository_id,
            String::from(digest),
        )
        .fetch_optional(executor)
        .await?;

        if let Some(row) = row {
            let manifest = Manifest {
                id: row.id.into(),
                registry_id: row.registry_id.into(),
                repository_id: row.repository_id.into(),
                blob_id: row.blob_id.into(),
                digest: row.digest.as_str().try_into()?,
                media_type: row.media_type.map(|v| v.as_str().into()),
                artifact_type: row.artifact_type.map(|v| v.as_str().into()),
            };

            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    pub async fn get_manifest_by_tag(
        executor: &mut PgConnection,
        registry_id: &Uuid,
        repository_id: &Uuid,
        tag: &str,
    ) -> Result<Option<Manifest>> {
        let row = sqlx::query!(
            r#"
SELECT m.id, m.registry_id, m.repository_id, m.blob_id, m.media_type, m.artifact_type, m.digest
FROM manifests m 
JOIN tags t 
ON m.id = t.manifest_id
WHERE m.registry_id = $1 AND m.repository_id = $2 AND t.name = $3
            "#,
            registry_id,
            repository_id,
            tag,
        )
        .fetch_optional(executor)
        .await?;

        if let Some(row) = row {
            let manifest = Manifest {
                id: row.id.into(),
                registry_id: row.registry_id.into(),
                repository_id: row.repository_id.into(),
                blob_id: row.blob_id.into(),
                digest: row.digest.as_str().try_into()?,
                media_type: row.media_type.map(|v| v.as_str().into()),
                artifact_type: row.artifact_type.map(|v| v.as_str().into()),
            };

            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    pub async fn insert_manifest(executor: &mut PgConnection, manifest: &Manifest) -> Result<()> {
        sqlx::query!(
            r#"
INSERT INTO manifests ( id, registry_id, repository_id, blob_id, media_type, artifact_type, digest )
VALUES ( $1, $2, $3, $4, $5, $6, $7 )
            "#,
            manifest.id,
            manifest.registry_id,
            manifest.repository_id,
            manifest.blob_id,
            manifest.media_type.clone().map(String::from),
            manifest.artifact_type.clone().map(String::from),
            String::from(&manifest.digest),
        )
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn delete_manifest(executor: &mut PgConnection, manifest_id: &Uuid) -> Result<()> {
        match sqlx::query!(
            r#"
DELETE FROM manifests
WHERE id = $1
            "#,
            manifest_id
        )
        .execute(executor)
        .await
        {
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
        let parents: Vec<Uuid> = std::iter::repeat(parent)
            .map(Clone::clone)
            .take(children.len())
            .collect::<Vec<_>>();
        let children: Vec<Uuid> = children.into_iter().map(Clone::clone).collect::<Vec<_>>();
        sqlx::query!(
            r#"
INSERT INTO layers(manifest, blob)
SELECT * FROM UNNEST($1::uuid[], $2::uuid[])
            "#,
            &parents[..],
            &children[..],
        )
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn delete_image_layers(executor: &mut PgConnection, parent: &Uuid) -> Result<()> {
        sqlx::query!(
            r#"
DELETE FROM layers 
WHERE manifest = $1
            "#,
            &parent
        )
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn associate_index_manifests(
        executor: &mut PgConnection,
        parent: &Uuid,
        children: Vec<&Uuid>,
    ) -> Result<()> {
        let parents: Vec<Uuid> = std::iter::repeat(parent)
            .map(Clone::clone)
            .take(children.len())
            .collect::<Vec<_>>();
        let children: Vec<Uuid> = children.into_iter().map(Clone::clone).collect::<Vec<_>>();
        sqlx::query!(
            r#"
INSERT INTO index_manifests(parent_manifest, child_manifest)
SELECT * FROM UNNEST($1::uuid[], $2::uuid[])
            "#,
            &parents[..],
            &children[..],
        )
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn delete_index_manifests(executor: &mut PgConnection, parent: &Uuid) -> Result<()> {
        sqlx::query!(
            r#"
DELETE FROM index_manifests 
WHERE parent_manifest = $1
            "#,
            &parent
        )
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn upsert_tag(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        manifest_id: &Uuid,
        tag: &str,
    ) -> Result<()> {
        sqlx::query!(
            r#"UPSERT INTO tags ( name, repository_id, manifest_id ) VALUES ( $1, $2, $3 )"#,
            tag,
            repository_id,
            manifest_id,
        )
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn get_tags(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        n: Option<i64>,
        last: Option<String>,
    ) -> Result<Vec<Tag>> {
        match (n, last) {
            (Some(n), Some(last)) => {
                Queries::get_tags_n_last(executor, repository_id, n, last.as_str()).await
            }
            (Some(n), None) => Queries::get_tags_n(executor, repository_id, n).await,
            (None, Some(_)) => Err(Error::MissingQueryParameter("n")),
            (None, None) => Queries::get_tags_all(executor, repository_id).await,
        }
    }

    async fn get_tags_all(executor: &mut PgConnection, repository_id: &Uuid) -> Result<Vec<Tag>> {
        let mut tags = Vec::new();

        let rows = sqlx::query!(
            r#"
SELECT t.manifest_id, t.name, m.digest
FROM tags t
JOIN manifests m 
ON t.manifest_id = m.id
WHERE m.repository_id = $1
ORDER BY name DESC
            "#,
            repository_id,
        )
        .fetch(executor);

        tokio::pin!(rows);

        while let Some(row) = rows.next().await {
            let row = row?;
            tags.push(Tag {
                manifest_id: row.manifest_id,
                name: row.name,
                digest: row.digest.as_str().try_into()?,
            })
        }

        Ok(tags)
    }

    async fn get_tags_n(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        n: i64,
    ) -> Result<Vec<Tag>> {
        let mut tags = Vec::new();

        let rows = sqlx::query!(
            r#"
SELECT t.manifest_id, t.name, m.digest
FROM tags t
JOIN manifests m 
ON t.manifest_id = m.id
WHERE m.repository_id = $1
ORDER BY name DESC
LIMIT $2
            "#,
            repository_id,
            n,
        )
        .fetch(executor);

        tokio::pin!(rows);

        while let Some(row) = rows.next().await {
            let row = row?;
            tags.push(Tag {
                manifest_id: row.manifest_id,
                name: row.name,
                digest: row.digest.as_str().try_into()?,
            })
        }

        Ok(tags)
    }

    async fn get_tags_n_last(
        executor: &mut PgConnection,
        repository_id: &Uuid,
        n: i64,
        last: &str,
    ) -> Result<Vec<Tag>> {
        let mut tags = Vec::new();

        let rows = sqlx::query!(
            r#"
SELECT t.manifest_id, t.name, m.digest
FROM tags t
JOIN manifests m 
WHERE (t.repository_id, t.name) > ($2, $3)
            "#,
            repository_id,
            repository_id,
            last,
            n,
        )
        .fetch(executor);

        tokio::pin!(rows);

        while let Some(row) = rows.next().await {
            let row = row?;
            tags.push(Tag {
                manifest_id: row.manifest_id,
                name: row.name,
                digest: row.digest.as_str().try_into()?,
            })
        }

        Ok(tags)
    }

    pub async fn delete_tags_by_manifest_id(
        executor: &mut PgConnection,
        manifest_id: &Uuid,
    ) -> Result<()> {
        sqlx::query!(
            r#"
DELETE FROM tags
WHERE manifest_id = $1
            "#,
            &manifest_id,
        )
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn get_chunks(
        executor: &mut PgConnection,
        session: &UploadSession,
    ) -> Result<Vec<Chunk>> {
        Ok(sqlx::query_as!(
            Chunk,
            r#"
SELECT e_tag, chunk_number
FROM chunks
WHERE upload_session_uuid = $1
ORDER BY chunk_number
            "#,
            session.uuid,
        )
        .fetch_all(executor)
        .await?)
    }
}

// PoolConnection<Postgres>-based metadata queries.
impl PostgresMetadataConn {
    pub async fn insert_registry(&mut self, name: &String) -> Result<Registry> {
        Ok(sqlx::query_as!(
            Registry,
            r#"
INSERT INTO registries ( name )
VALUES ( $1 )
RETURNING id, name
            "#,
            name,
        )
        .fetch_one(&mut *self.conn)
        .await?)
    }

    pub async fn get_registry(&mut self, name: impl ToString) -> Result<Registry> {
        Ok(sqlx::query_as!(
            Registry,
            r#"
SELECT id, name 
FROM registries
WHERE name = $1
            "#,
            name.to_string(),
        )
        .fetch_one(&mut *self.conn)
        .await?)
    }

    pub async fn insert_repository(
        &mut self,
        registry_id: &Uuid,
        name: &String,
    ) -> Result<Repository> {
        Ok(sqlx::query_as!(
            Repository,
            r#"
INSERT INTO repositories ( name, registry_id )
VALUES ( $1, $2 )
RETURNING id, name, registry_id
            "#,
            name,
            registry_id
        )
        .fetch_one(&mut *self.conn)
        .await?)
    }

    pub async fn get_repository(
        &mut self,
        registry: &Uuid,
        repository: &String,
    ) -> Result<Repository> {
        Ok(sqlx::query_as!(
            Repository,
            r#"
SELECT rep.id, rep.name, rep.registry_id
FROM repositories rep
JOIN registries reg
ON reg.id = rep.registry_id
WHERE reg.id = $1 AND rep.name = $2
            "#,
            registry,
            repository,
        )
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
        Ok(sqlx::query!(
            r#"
SELECT exists(
    SELECT 1
    FROM repositories
    WHERE registry_id = $1 AND name = $2
) as "exists!"
            "#,
            registry_id,
            String::from(name),
        )
        .fetch_one(&mut *self.conn)
        .await?
        .exists)
    }

    pub async fn blob_exists(&mut self, registry_id: &Uuid, digest: &OciDigest) -> Result<bool> {
        Ok(sqlx::query!(
            r#"
SELECT exists(
    SELECT 1
    FROM blobs
    WHERE registry_id = $1 AND digest = $2
) as "exists!"
            "#,
            registry_id,
            String::from(digest),
        )
        .fetch_one(&mut *self.conn)
        .await?
        .exists)
    }

    pub async fn get_manifest(
        &mut self,
        registry_id: &Uuid,
        repository_id: &Uuid,
        manifest_ref: &ManifestRef,
    ) -> Result<Option<Manifest>> {
        let manifest = match manifest_ref {
            ManifestRef::Digest(d) => {
                self.get_manifest_by_digest(registry_id, repository_id, &d)
                    .await?
            }
            ManifestRef::Tag(s) => {
                self.get_manifest_by_tag(registry_id, repository_id, &s)
                    .await?
            }
        };

        Ok(manifest)
    }

    pub async fn get_manifest_by_digest(
        &mut self,
        registry_id: &Uuid,
        repository_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Option<Manifest>> {
        Queries::get_manifest_by_digest(&mut *self.conn, registry_id, repository_id, digest).await
    }

    pub async fn get_manifest_by_tag(
        &mut self,
        registry_id: &Uuid,
        repository_id: &Uuid,
        tag: &str,
    ) -> Result<Option<Manifest>> {
        Queries::get_manifest_by_tag(&mut *self.conn, registry_id, repository_id, tag).await
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
        let session = sqlx::query_as!(
            UploadSession,
            r#"
INSERT INTO upload_sessions ( digest_state )
VALUES ( $1 )
RETURNING uuid, start_date, upload_id, chunk_number, last_range_end, digest_state as "digest_state: Json<DigestState>"
            "#,
            serde_json::to_value(state)?,
        )
        .fetch_one(&mut *self.conn)
        .await?;

        Ok(session)
    }

    pub async fn get_session(&mut self, uuid: &Uuid) -> Result<UploadSession> {
        let session = sqlx::query_as!(
            UploadSession,
            r#"
SELECT uuid, start_date, chunk_number, last_range_end, upload_id, digest_state as "digest_state: Json<DigestState>"
FROM upload_sessions
WHERE uuid = $1
            "#,
            uuid,
            )
            .fetch_one(&mut *self.conn)
            .await?;

        Ok(session)
    }

    pub async fn update_session(&mut self, session: &UploadSession) -> Result<()> {
        sqlx::query_as!(
            UploadSession,
            r#"
UPDATE upload_sessions
SET upload_id = $2, chunk_number = $3, last_range_end = $4, digest_state = $5
WHERE uuid = $1
            "#,
            session.uuid,
            session.upload_id,
            session.chunk_number,
            session.last_range_end,
            serde_json::to_value(session.digest_state.as_ref())?,
        )
        .execute(&mut *self.conn)
        .await?;

        Ok(())
    }

    pub async fn delete_session(&mut self, session: &UploadSession) -> Result<()> {
        // delete chunks
        sqlx::query!(
            r#"
DELETE
FROM chunks
WHERE upload_session_uuid = $1
            "#,
            session.uuid,
        )
        .execute(&mut *self.conn)
        .await?;

        // delete session
        sqlx::query!(
            r#"
DELETE
FROM upload_sessions
WHERE uuid = $1
            "#,
            session.uuid,
        )
        .execute(&mut *self.conn)
        .await?;

        Ok(())
    }

    pub async fn get_chunks(&mut self, session: &UploadSession) -> Result<Vec<Chunk>> {
        Queries::get_chunks(&mut *self.conn, session).await
    }

    pub async fn insert_chunk(&mut self, session: &UploadSession, chunk: &Chunk) -> Result<()> {
        sqlx::query!(
            r#"
INSERT INTO chunks (chunk_number, upload_session_uuid, e_tag)
VALUES ( $1, $2, $3 )
            "#,
            chunk.chunk_number,
            session.uuid,
            chunk.e_tag,
        )
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

    pub async fn get_manifest_by_digest(
        &mut self,
        registry_id: &Uuid,
        repository_id: &Uuid,
        digest: &OciDigest,
    ) -> Result<Option<Manifest>> {
        let tx = self.tx.as_mut().ok_or(Error::PostgresMetadataTxInactive)?;
        Queries::get_manifest_by_digest(&mut **tx, registry_id, repository_id, digest).await
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
