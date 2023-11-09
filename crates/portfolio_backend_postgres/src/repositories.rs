use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use portfolio_core::errors::Result;
use portfolio_core::registry::BoxedBlobStore;
use portfolio_core::registry::BoxedManifestStore;
use portfolio_core::registry::BoxedRepositoryStore;
use portfolio_core::registry::BoxedUploadSessionStore;
use portfolio_core::registry::RepositoryStore as RepositoryStoreT;
use portfolio_core::registry::RepositoryStoreManager;
use portfolio_objectstore::{Config as ObjectStoreConfig, ObjectStore};

use super::blobs::PgBlobStore;
use super::errors::Error;
use super::manifests::PgManifestStore;
use super::metadata::Repository;
use super::metadata::{PostgresConfig, PostgresMetadataPool};
use super::upload_sessions::PgSessionStore;

/// [`RepositoryStore`](portfolio_core::registry::RepositoryStore) implementation.
///
/// PgRepository is an implementation of
/// [`RepositoryStore`](portfolio_core::registry::RepositoryStore) that makes use of a Postgres
/// database for managing metadata and is generic over
/// [`ObjectStore`](portfolio_objectstore::ObjectStore).
#[derive(Clone)]
pub struct PgRepository {
    objects: Arc<dyn ObjectStore>,
    metadata: PostgresMetadataPool,

    repository: Repository,
}

impl PgRepository {
    pub async fn get(
        name: &str,
        metadata: PostgresMetadataPool,
        objects: Arc<dyn ObjectStore>,
    ) -> Result<Option<Self>> {
        if let Some(repository) = metadata.get_conn().await?.get_repository(name).await? {
            Ok(Some(Self {
                objects,
                metadata,
                repository,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_or_insert(
        name: &str,
        metadata: PostgresMetadataPool,
        objects: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let mut conn = metadata.get_conn().await?;

        let repository = match conn.get_repository(name).await? {
            Some(repository) => repository,
            None => conn.insert_repository(name).await?,
        };

        Ok(Self {
            objects,
            metadata,
            repository,
        })
    }
}

#[async_trait]
impl RepositoryStoreT for PgRepository {
    fn name(&self) -> &str {
        self.repository.name.as_str()
    }

    fn get_manifest_store(&self) -> BoxedManifestStore {
        let blobstore = PgBlobStore::new(self.metadata.clone(), self.objects.clone());
        Box::new(PgManifestStore::new(blobstore, self.repository.clone()))
    }

    fn get_blob_store(&self) -> BoxedBlobStore {
        Box::new(PgBlobStore::new(
            self.metadata.clone(),
            self.objects.clone(),
        ))
    }

    fn get_upload_session_store(&self) -> BoxedUploadSessionStore {
        Box::new(PgSessionStore::new(self.metadata.clone()))
    }
}

/// [`RepositoryStoreManager`](portfolio_core::registry::RepositoryStoreManager) implementation.
///
/// Manages initialization and retrieval of [`PgRepository`] instances.
#[derive(Clone)]
pub struct PgRepositoryFactory {
    metadata: PostgresMetadataPool,
    objects: Arc<dyn ObjectStore>,
}

#[async_trait]
impl RepositoryStoreManager for PgRepositoryFactory {
    async fn get(&self, name: &str) -> Result<Option<BoxedRepositoryStore>> {
        if let Some(s) =
            PgRepository::get(name, self.metadata.clone(), self.objects.clone()).await?
        {
            Ok(Some(Box::new(s)))
        } else {
            Ok(None)
        }
    }

    async fn create(&self, name: &str) -> Result<BoxedRepositoryStore> {
        Ok(Box::new(
            PgRepository::get_or_insert(name, self.metadata.clone(), self.objects.clone()).await?,
        ))
    }
}

/// Holds configuration necessary to initialize an instance of [`PgRepositoryFactory`].
#[derive(Clone, Deserialize)]
pub struct PgRepositoryConfig {
    postgres: PostgresConfig,
    objects: ObjectStoreConfig,
}

impl PgRepositoryConfig {
    pub async fn get_manager(&self) -> Result<PgRepositoryFactory> {
        Ok(PgRepositoryFactory {
            metadata: self.postgres.new_metadata().await?,
            objects: self.objects.new_objects().await.map_err(Error::from)?,
        })
    }
}
