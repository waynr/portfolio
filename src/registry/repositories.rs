use serde::Serialize;
use uuid::Uuid;

use crate::errors::Result;
use crate::metadata::{PostgresMetadataPool, Repository as RepositoryMetadata};
use crate::objects::ObjectStore;
use crate::registry::blobs::BlobStore;
use crate::registry::manifests::ManifestStore;
use crate::registry::UploadSession;

#[derive(Clone)]
pub struct Repository<O>
where
    O: ObjectStore,
{
    objects: O,
    metadata: PostgresMetadataPool,

    repository: RepositoryMetadata,
}

impl<O> Repository<O>
where
    O: ObjectStore,
{
    pub async fn get(
        name: &str,
        metadata: PostgresMetadataPool,
        objects: O,
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
        objects: O,
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

    pub fn name(&self) -> &str {
        self.repository.name.as_str()
    }

    pub fn get_manifest_store(&self) -> ManifestStore<O> {
        let blobstore = BlobStore::new(self.metadata.clone(), self.objects.clone());
        ManifestStore::new(blobstore, &self.repository)
    }

    pub async fn get_tags(&self, n: Option<i64>, last: Option<String>) -> Result<TagsList> {
        let mut conn = self.metadata.get_conn().await?;

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

    pub fn get_blob_store(&self) -> BlobStore<O> {
        BlobStore::new(self.metadata.clone(), self.objects.clone())
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
        let mut tx = self.metadata.get_tx().await?;

        tx.delete_chunks(&session.uuid).await?;
        tx.delete_session(session).await?;

        tx.commit().await?;

        Ok(())
    }

    pub async fn create_repository(&self, name: &String) -> Result<RepositoryMetadata> {
        self.metadata
            .get_conn()
            .await?
            .insert_repository(name)
            .await
    }
}

#[derive(Serialize)]
pub struct TagsList {
    name: String,
    tags: Vec<String>,
}
