use async_trait::async_trait;
use serde::Deserialize;

use portfolio::registry::RepositoryStoreManager;

use super::errors::{Error, Result};
use super::metadata::{PostgresConfig, PostgresMetadataPool};
use super::objects::{S3Config, S3};
use super::repositories::PgS3Repository;

#[derive(Clone, Deserialize)]
pub struct PgS3RepositoryConfig {
    postgres: PostgresConfig,
    s3: S3Config,
}

impl PgS3RepositoryConfig {
    pub async fn get_manager(&self) -> Result<PgS3RepositoryFactory> {
        Ok(PgS3RepositoryFactory {
            metadata: self.postgres.new_metadata().await?,
            objects: self.s3.new_objects().await?,
        })
    }
}

#[derive(Clone)]
pub struct PgS3RepositoryFactory {
    metadata: PostgresMetadataPool,
    objects: S3,
}

#[async_trait]
impl RepositoryStoreManager for PgS3RepositoryFactory {
    type RepositoryStore = PgS3Repository;
    type Error = Error;

    async fn get(&self, name: &str) -> Result<Option<Self::RepositoryStore>> {
        PgS3Repository::get(name, self.metadata.clone(), self.objects.clone()).await
    }

    async fn create(&self, name: &str) -> Result<Self::RepositoryStore> {
        Ok(
            PgS3Repository::get_or_insert(name, self.metadata.clone(), self.objects.clone())
                .await?,
        )
    }
}
