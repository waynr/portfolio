use uuid::Uuid;

use crate::errors::Result;
use crate::metadata::{PostgresMetadata, Registry as RegistryMetadata};
use crate::objects::ObjectStore;
use crate::registry::blobs::BlobStore;
use crate::registry::UploadSession;

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
    pub async fn new(name: String, metadata: PostgresMetadata, objects: O) -> Result<Self> {
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

    pub fn get_blob_store(&self) -> BlobStore<O> {
        BlobStore::new(self.metadata.clone(), self.objects.clone(), &self.registry)
    }

    pub async fn new_upload_session(&self) -> Result<UploadSession> {
        self.metadata.new_upload_session().await
    }

    pub async fn get_upload_session(&self, session_uuid: &Uuid) -> Result<UploadSession> {
        self.metadata
            .get_session(session_uuid)
            .await
    }

    pub async fn delete_session(&self, session: &UploadSession) -> Result<()> {
        self.metadata.delete_session(session).await
    }
}
