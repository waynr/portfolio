use serde::Serialize;
use uuid::Uuid;

use crate::errors::Result;
use crate::metadata::{
    PostgresMetadataPool, Registry as RegistryMetadata, Repository as RepositoryMetadata,
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
    metadata: PostgresMetadataPool,

    registry: RegistryMetadata,
}

impl<O> Registry<O>
where
    O: ObjectStore,
{
    pub async fn new(name: &str, metadata: PostgresMetadataPool, objects: O) -> Result<Self> {
        let mut conn = metadata.get_conn().await?;
        let registry = conn.get_registry(name).await?;
        Ok(Self {
            objects,
            metadata,
            registry,
        })
    }

    pub async fn repository_exists(&self, name: &String) -> Result<bool> {
        let mut conn = self.metadata.get_conn().await?;
        conn.repository_exists(&self.registry.id, name).await
    }

    pub async fn get_repository(&self, name: &String) -> Result<Repository<O>> {
        let mut conn = self.metadata.get_conn().await?;
        let repository = conn.get_repository(&self.registry.id, name).await?;

        Ok(Repository {
            registry: self.clone(),
            repository,
        })
    }

    pub fn get_blob_store(&self) -> BlobStore<O> {
        BlobStore::new(self.metadata.clone(), self.objects.clone(), &self.registry)
    }

    pub async fn new_upload_session(&self) -> Result<UploadSession> {
        self.metadata.get_conn().await?.new_upload_session().await
    }

    pub async fn get_upload_session(&self, session_uuid: &Uuid) -> Result<UploadSession> {
        self.metadata
            .get_conn()
            .await?
            .get_session(session_uuid)
            .await
    }

    pub async fn delete_session(&self, session: &UploadSession) -> Result<()> {
        self.metadata
            .get_conn()
            .await?
            .delete_session(session)
            .await
    }

    pub async fn create_repository(&self, name: &String) -> Result<RepositoryMetadata> {
        self.metadata
            .get_conn()
            .await?
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
        let blobstore = self.registry.get_blob_store();
        ManifestStore::new(blobstore, &self.repository)
    }

    pub async fn get_tags(&self, n: Option<i64>, last: Option<String>) -> Result<TagsList> {
        let mut conn = self.registry.metadata.get_conn().await?;

        Ok(TagsList {
            name: self.repository.name.clone(),
            tags: conn
                .get_tags(&self.repository.id, n, last)
                .await?
                .into_iter()
                .map(|t| t.name)
                .collect(),
        })
    }
}

#[derive(Serialize)]
pub struct TagsList {
    name: String,
    tags: Vec<String>,
}
