use async_trait::async_trait;
use uuid::Uuid;

use portfolio_core::registry::{UploadSession, UploadSessionStore};
use portfolio_core::Result;

use super::metadata::PostgresMetadataPool;

#[derive(Clone)]
pub struct PgSessionStore {
    metadata: PostgresMetadataPool,
}

impl PgSessionStore {
    pub fn new(metadata: PostgresMetadataPool) -> Self {
        Self { metadata }
    }
}

#[async_trait]
impl UploadSessionStore for PgSessionStore {
    async fn new_upload_session(&self) -> Result<Box<dyn UploadSession + Send + Sync>> {
        Ok(Box::new(
            self.metadata.get_conn().await?.new_upload_session().await?,
        ))
    }

    async fn get_upload_session(
        &self,
        session_uuid: &Uuid,
    ) -> Result<Box<dyn UploadSession + Send + Sync>> {
        Ok(Box::new(
            self.metadata
                .get_conn()
                .await?
                .get_session(session_uuid)
                .await?,
        ))
    }

    async fn delete_session(&self, session_uuid: &Uuid) -> Result<()> {
        let mut tx = self.metadata.get_tx().await?;

        tx.delete_chunks(session_uuid).await?;
        tx.delete_session(session_uuid).await?;

        tx.commit().await?;

        Ok(())
    }
}
