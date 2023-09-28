use uuid::Uuid;

use crate::errors::Result;
use crate::metadata::{
    PostgresMetadata, Registry as RegistryMetadata, Repository as RepositoryMetadata,
};
use crate::objects::ObjectStore;
use crate::registry::blobs::BlobStore;
use crate::registry::manifests::ManifestStore;
use crate::registry::UploadSession;

#[derive(Clone)]
pub struct Registry<O>
where
    O: ObjectStore,
{
    objects: O,
    metadata: PostgresMetadata,

    registry: RegistryMetadata,
}

impl<O> Registry<O>
where
    O: ObjectStore,
{
    pub async fn new(name: &str, metadata: PostgresMetadata, objects: O) -> Result<Self> {
        let registry = metadata.get_registry(name).await?;
        Ok(Self {
            objects,
            metadata,
            registry,
        })
    }

    pub async fn repository_exists(&self, name: &String) -> Result<bool> {
        self.metadata
            .repository_exists(&self.registry.id, name)
            .await
    }

    pub async fn get_repository(&self, name: &String) -> Result<Repository<O>> {
        let repository = self
            .metadata
            .get_repository(&self.registry.id, name)
            .await?;

        Ok(Repository {
            registry: self.clone(),
            repository,
        })
    }

    pub fn get_blob_store(&self) -> BlobStore<O> {
        BlobStore::new(self.metadata.clone(), self.objects.clone(), &self.registry)
    }

    pub async fn new_upload_session(&self) -> Result<UploadSession> {
        self.metadata.new_upload_session().await
    }

    pub async fn get_upload_session(&self, session_uuid: &Uuid) -> Result<UploadSession> {
        self.metadata.get_session(session_uuid).await
    }

    pub async fn delete_session(&self, session: &UploadSession) -> Result<()> {
        self.metadata.delete_session(session).await
    }

    pub async fn create_repository(&self, name: &String) -> Result<RepositoryMetadata> {
        self.metadata
            .insert_repository(&self.registry.id, name)
            .await
    }
}

#[derive(Clone)]
pub struct Repository<O>
where
    O: ObjectStore,
{
    registry: Registry<O>,
    repository: RepositoryMetadata,
}

impl<O> Repository<O>
where
    O: ObjectStore,
{
    pub fn get_manifest_store(&self) -> ManifestStore<O> {
        ManifestStore::new(
            self.registry.metadata.clone(),
            self.registry.objects.clone(),
            &self.repository,
        )
    }
}
