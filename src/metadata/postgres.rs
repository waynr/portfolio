use serde::Deserialize;
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::Pool;
use sqlx::types::Json;
use uuid::Uuid;

use crate::errors::Result;
use crate::http::blobs::UploadSession;
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
    pub async fn insert_blob(&self, digest: &str) -> Result<i64> {
        let mut conn = self.pool.acquire().await?;
        let record = sqlx::query!(
            r#"
INSERT INTO blobs ( digest )
VALUES ( $1 )
RETURNING id
            "#,
            digest,
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
RETURNING uuid, start_date, digest_state as "digest_state: Option<Json<DigestState>>"
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
SELECT uuid, start_date, digest_state as "digest_state: Option<Json<DigestState>>"
FROM upload_sessions
WHERE uuid = $1
            "#,
            uuid,
            )
            .fetch_one(&mut conn)
            .await?;

        Ok(session)
    }

    pub async fn delete_session(&self, uuid: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        let session = sqlx::query!(
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
