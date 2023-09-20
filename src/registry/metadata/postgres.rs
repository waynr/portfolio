use serde::Deserialize;
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::Pool;
use sqlx::types::Json;
use uuid::Uuid;

use crate::OciDigest;
use crate::errors::{Error, Result};
use crate::metadata::{Blob, Registry, Repository};
use crate::registry::{Chunk, UploadSession};
use crate::{DigestState, RegistryDefinition};

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

// basic DB interaction methods
impl PostgresMetadata {
    pub async fn insert_registry(&self, name: &String) -> Result<Registry> {
        let mut conn = self.pool.acquire().await?;
        Ok(sqlx::query_as!(
            Registry,
            r#"
INSERT INTO registries ( name )
VALUES ( $1 )
RETURNING id, name
            "#,
            name,
        )
        .fetch_one(&mut conn)
        .await?)
    }

    pub async fn get_registry(&self, name: impl ToString) -> Result<Registry> {
        let mut conn = self.pool.acquire().await?;
        Ok(sqlx::query_as!(
            Registry,
            r#"
SELECT id, name 
FROM registries
WHERE name = $1
            "#,
            name.to_string(),
        )
        .fetch_one(&mut conn)
        .await?)
    }

    pub async fn insert_repository(&self, registry_id: &Uuid, name: &String) -> Result<Repository> {
        let mut conn = self.pool.acquire().await?;
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
        .fetch_one(&mut conn)
        .await?)
    }

    pub async fn get_repository(&self, registry: &Uuid, repository: &String) -> Result<Repository> {
        let mut conn = self.pool.acquire().await?;
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
        .fetch_one(&mut conn)
        .await?)
    }

    pub async fn insert_blob(&self, registry_id: &Uuid, digest: &OciDigest) -> Result<Uuid> {
        let mut conn = self.pool.acquire().await?;
        let record = sqlx::query!(
            r#"
INSERT INTO blobs ( digest, registry_id )
VALUES ( $1, $2 )
RETURNING id
            "#,
            String::from(digest),
            registry_id,
        )
        .fetch_one(&mut conn)
        .await?;

        Ok(record.id)
    }

    pub async fn get_blob(&self, registry_id: &Uuid, digest: &OciDigest) -> Result<Option<Blob>> {
        let mut conn = self.pool.acquire().await?;
        Ok(sqlx::query_as!(
            Blob,
            r#"
SELECT id, digest, registry_id
FROM blobs
WHERE registry_id = $1 AND digest = $2
            "#,
            registry_id,
            String::from(digest),
        )
        .fetch_optional(&mut conn)
        .await?)
    }

    pub async fn blob_exists(&self, registry_id: &Uuid, digest: &OciDigest) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
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
        .fetch_one(&mut conn)
        .await?
        .exists)
    }

    pub async fn new_upload_session(&self) -> Result<UploadSession> {
        let mut conn = self.pool.acquire().await?;
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
        .fetch_one(&mut conn)
        .await?;

        Ok(session)
    }

    pub async fn get_session(&self, uuid: Uuid) -> Result<UploadSession> {
        let mut conn = self.pool.acquire().await?;
        let session = sqlx::query_as!(
            UploadSession,
            r#"
SELECT uuid, start_date, chunk_number, last_range_end, upload_id, digest_state as "digest_state: Json<DigestState>"
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
SET upload_id = $2, chunk_number = $3, last_range_end = $4, digest_state = $5
WHERE uuid = $1
            "#,
            session.uuid,
            session.upload_id,
            session.chunk_number,
            session.last_range_end,
            serde_json::to_value(session.digest_state.as_ref())?,
        )
        .execute(&mut conn)
        .await?;

        Ok(())
    }

    pub async fn delete_session(&self, session: &UploadSession) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        // delete chunks
        sqlx::query!(
            r#"
DELETE
FROM chunks
WHERE upload_session_uuid = $1
            "#,
            session.uuid,
        )
        .execute(&mut conn)
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
        .execute(&mut conn)
        .await?;

        Ok(())
    }

    pub async fn get_chunks(&self, session: &UploadSession) -> Result<Vec<Chunk>> {
        let mut conn = self.pool.acquire().await?;
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
            .fetch_all(&mut conn)
            .await?)
    }

    pub async fn insert_chunk(&self, session: &UploadSession, chunk: &Chunk) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query!(
            r#"
INSERT INTO chunks (chunk_number, upload_session_uuid, e_tag)
VALUES ( $1, $2, $3 )
            "#,
            chunk.chunk_number,
            session.uuid,
            chunk.e_tag,
            )
            .execute(&mut conn)
            .await?;

        Ok(())
    }
}

// higher level DB interaction methods
impl PostgresMetadata {
    pub async fn initialize_static_registries(
        &mut self,
        registries: Vec<RegistryDefinition>,
    ) -> Result<()> {
        for registry_config in registries {
            let registry = match self.get_registry(&registry_config.name).await {
                Ok(r) => r,
                Err(e) => match e {
                    Error::SQLXError(ref source) => match source {
                        sqlx::Error::RowNotFound => {
                            println!("registry not found!");
                            self.insert_registry(&registry_config.name).await?
                        }
                        _ => return Err(e),
                    },
                    _ => return Err(e),
                },
            };

            for repository_config in registry_config.repositories {
                match self
                    .get_repository(&registry.id, &repository_config.name)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => match e {
                        Error::SQLXError(ref source) => match source {
                            sqlx::Error::RowNotFound => {
                                println!("repository not found!");
                                self.insert_repository(&registry.id, &repository_config.name)
                                    .await?
                            }
                            _ => return Err(e),
                        },
                        _ => return Err(e),
                    },
                };
            }
        }
        Ok(())
    }
}
