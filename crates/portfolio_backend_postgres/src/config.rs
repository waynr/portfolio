use async_trait::async_trait;
use serde::Deserialize;

use portfolio_core::registry::RepositoryStoreManager;
use portfolio_objectstore::{S3Config, S3};

use super::errors::{Error, Result};
use super::metadata::{PostgresConfig, PostgresMetadataPool};
use super::repositories::PgRepository;

#[derive(Clone, Deserialize)]
pub struct PgRepositoryConfig {
    postgres: PostgresConfig,
    s3: S3Config,
}

impl PgRepositoryConfig {
    pub async fn get_manager(&self) -> Result<PgRepositoryFactory> {
        Ok(PgRepositoryFactory {
            metadata: self.postgres.new_metadata().await?,
            objects: self.s3.new_objects().await?,
        })
    }
}

#[derive(Clone)]
pub struct PgRepositoryFactory {
    metadata: PostgresMetadataPool,
    objects: S3,
}

#[async_trait]
impl RepositoryStoreManager for PgRepositoryFactory {
    type RepositoryStore = PgRepository;
    type Error = Error;

    async fn get(&self, name: &str) -> Result<Option<Self::RepositoryStore>> {
        PgRepository::get(name, self.metadata.clone(), self.objects.clone()).await
    }

    async fn create(&self, name: &str) -> Result<Self::RepositoryStore> {
        Ok(
            PgRepository::get_or_insert(name, self.metadata.clone(), self.objects.clone())
                .await?,
        )
    }
}
