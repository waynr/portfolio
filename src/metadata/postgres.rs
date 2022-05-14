use serde::Deserialize;
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::types::Json;
use sqlx::Pool;
use uuid::Uuid;

use crate::errors::{Error, Result};
use crate::http::blobs::UploadSession;
use crate::metadata::{Blob, Registry, Repository};
use crate::objects::ChunkInfo;
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

    pub async fn insert_blob(&self, registry_id: &Uuid, digest: &str, id: &Uuid) -> Result<Uuid> {
        let mut conn = self.pool.acquire().await?;
        let record = sqlx::query!(
            r#"
INSERT INTO blobs ( id, digest, registry_id )
VALUES ( $1, $2, $3 )
RETURNING id
            "#,
            id,
            digest,
            registry_id,
        )
        .fetch_one(&mut conn)
        .await?;

        Ok(record.id)
    }

    pub async fn get_blob(&self, registry_id: &Uuid, digest: &str) -> Result<Blob> {
        let mut conn = self.pool.acquire().await?;
        Ok(sqlx::query_as!(
            Blob,
            r#"
SELECT id, digest, registry_id
FROM blobs
WHERE registry_id = $1 AND digest = $2
            "#,
            registry_id,
            digest,
        )
        .fetch_one(&mut conn)
        .await?)
    }

    pub async fn blob_exists(&self, registry_id: &Uuid, digest: &str) -> Result<bool> {
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
            digest,
        )
        .fetch_one(&mut conn)
        .await?.exists)
    }

    pub async fn new_upload_session(&self) -> Result<UploadSession> {
        let mut conn = self.pool.acquire().await?;
        let state = DigestState::default();
        let session = sqlx::query_as!(
            UploadSession,
            r#"
INSERT INTO upload_sessions ( digest_state )
VALUES ( $1 )
RETURNING uuid, start_date, digest_state as "digest_state: Json<DigestState>", chunk_info as "chunk_info: Json<ChunkInfo>"
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
SELECT uuid, start_date, digest_state as "digest_state: Json<DigestState>", chunk_info as "chunk_info: Json<ChunkInfo>"
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
