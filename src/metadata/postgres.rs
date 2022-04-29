use serde::Deserialize;
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::types::Json;
use sqlx::Pool;
use uuid::Uuid;

use crate::errors::Result;
use crate::http::blobs::UploadSession;
use crate::metadata::{Registry, Repository};
use crate::objects::ChunkInfo;
use crate::DigestState;

#[derive(Deserialize)]
pub struct PostgresConfig {
    connection_string: String,
}

impl PostgresConfig {
    pub async fn new_metadata(&self) -> Result<PostgresMetadata> {
        let pool = PgPoolOptions::new()
            .connect(&self.connection_string)
            .await?;
        Ok(PostgresMetadata { pool })
    }
}

pub struct PostgresMetadata {
    pool: Pool<Postgres>,
}

impl PostgresMetadata {
    pub async fn get_registry(&self, name: &String) -> Result<Registry> {
        let mut conn = self.pool.acquire().await?;
        Ok(sqlx::query_as!(
            Registry,
            r#"
SELECT id, name 
FROM registries
WHERE name = $1
            "#,
            name,
        )
        .fetch_one(&mut conn)
        .await?)
    }

    pub async fn insert_repository(&self, repository: &mut Repository) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        repository.id = sqlx::query!(
            r#"
INSERT INTO repositories ( name, registry_id )
VALUES ( $1, $2 )
RETURNING id
            "#,
            repository.name,
            repository.registry.id,
        )
        .fetch_one(&mut conn)
        .await?
        .id;

        Ok(())
    }

    pub async fn get_repository(&self, registry: &String, repository: &String) -> Result<Repository> {
        let mut conn = self.pool.acquire().await?;
        Ok(sqlx::query_as!(
            Repository,
            r#"
SELECT rep.id, rep.name 
FROM registries reg
JOIN repositories rep
ON reg.id = rep.registry_id
WHERE reg.name = $1 AND rep.name = $2
            "#,
            registry, repository,
        )
        .fetch_one(&mut conn)
        .await?)
    }

    pub async fn insert_blob(&self, digest: &str, object_key: &Uuid) -> Result<i64> {
        let mut conn = self.pool.acquire().await?;
        let record = sqlx::query!(
            r#"
INSERT INTO blobs ( digest, object_key )
VALUES ( $1, $2 )
RETURNING id
            "#,
            digest,
            object_key,
        )
        .fetch_one(&mut conn)
        .await?;

        Ok(record.id)
    }

    pub async fn new_upload_session(&self) -> Result<UploadSession> {
        let mut conn = self.pool.acquire().await?;
        let state: Option<Json<DigestState>> = None;
        let session = sqlx::query_as!(
            UploadSession,
            r#"
INSERT INTO upload_sessions ( digest_state )
VALUES ( $1 )
RETURNING uuid, start_date, digest_state as "digest_state: Option<Json<DigestState>>", chunk_info as "chunk_info: Option<Json<ChunkInfo>>"
            "#,
            serde_json::to_value(state)?,
        )
        .fetch_one(&mut conn)
        .await?;

        Ok(session)
    }

    pub async fn get_session(&self, uuid: Uuid) -> Result<UploadSession> {
        let mut conn = self.pool.acquire().await?;
        let session = sqlx::query_as!(
            UploadSession,
            r#"
SELECT uuid, start_date, digest_state as "digest_state: Option<Json<DigestState>>", chunk_info as "chunk_info: Option<Json<ChunkInfo>>"
FROM upload_sessions
WHERE uuid = $1
            "#,
            uuid,
            )
            .fetch_one(&mut conn)
            .await?;

        Ok(session)
    }

    pub async fn update_session(&self, session: &UploadSession) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query_as!(
            UploadSession,
            r#"
UPDATE upload_sessions
SET digest_state = $1, chunk_info = $2
WHERE uuid = $3
            "#,
            serde_json::to_value(session.digest_state.as_ref())?,
            serde_json::to_value(session.chunk_info.as_ref())?,
            session.uuid,
        )
        .execute(&mut conn)
        .await?;

        Ok(())
    }

    pub async fn delete_session(&self, uuid: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query!(
            r#"
DELETE
FROM upload_sessions
WHERE uuid = $1
            "#,
            uuid,
        )
        .execute(&mut conn)
        .await?;

        Ok(())
    }
}
