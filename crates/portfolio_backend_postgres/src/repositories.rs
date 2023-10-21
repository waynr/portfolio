use async_trait::async_trait;
use uuid::Uuid;

use portfolio_core::registry::{RepositoryStore, TagsList};
use portfolio_objectstore::S3;

use super::blobs::PgS3BlobStore;
use super::errors::{Error, Result};
use super::manifests::PgS3ManifestStore;
use super::metadata::PostgresMetadataPool;
use super::metadata::Repository;
use super::metadata::UploadSession;

#[derive(Clone)]
pub struct PgS3Repository {
    objects: S3,
    metadata: PostgresMetadataPool,

    repository: Repository,
}

impl PgS3Repository {
    pub async fn get(
        name: &str,
        metadata: PostgresMetadataPool,
        objects: S3,
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
        objects: S3,
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
impl RepositoryStore for PgS3Repository {
    type ManifestStore = PgS3ManifestStore;
    type BlobStore = PgS3BlobStore;
    type Error = Error;
    type UploadSession = UploadSession;

    fn name(&self) -> &str {
        self.repository.name.as_str()
    }

    fn get_manifest_store(&self) -> Self::ManifestStore {
        let blobstore = PgS3BlobStore::new(self.metadata.clone(), self.objects.clone());
        PgS3ManifestStore::new(blobstore, self.repository.clone())
    }

    fn get_blob_store(&self) -> Self::BlobStore {
        PgS3BlobStore::new(self.metadata.clone(), self.objects.clone())
    }

    async fn get_tags(&self, n: Option<i64>, last: Option<String>) -> Result<TagsList> {
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

    async fn new_upload_session(&self) -> Result<Self::UploadSession> {
        self.metadata.get_conn().await?.new_upload_session().await
    }

    async fn get_upload_session(&self, session_uuid: &Uuid) -> Result<Self::UploadSession> {
        self.metadata
            .get_conn()
            .await?
            .get_session(session_uuid)
            .await
    }

    async fn delete_session(&self, session_uuid: &Uuid) -> Result<()> {
        let mut tx = self.metadata.get_tx().await?;

        tx.delete_chunks(session_uuid).await?;
        tx.delete_session(session_uuid).await?;

        tx.commit().await?;

        Ok(())
    }
}
