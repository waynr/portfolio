use std::sync::Arc;

use hyper::body::Body;

use crate::{errors::Result, metadata::PostgresMetadata, objects::S3, registry::UploadSession};

pub struct BlobStore {
    metadata: Arc<PostgresMetadata>,
    objects: Arc<S3>,
}

impl BlobStore {
    pub fn new(metadata: Arc<PostgresMetadata>, objects: Arc<S3>) -> Self {
        Self { metadata, objects }
    }

    pub async fn resume(&self, mut session: UploadSession) -> Result<BlobWriter> {
        match session.upload_id {
            Some(_) => (),
            None => {
                self.objects
                    .clone()
                    .initiate_chunked_upload(&mut session)
                    .await?
            }
        };

        Ok(BlobWriter {
            metadata: self.metadata.clone(),
            objects: self.objects.clone(),
            session,
        })
    }
}

pub struct BlobWriter {
    metadata: Arc<PostgresMetadata>,
    objects: Arc<S3>,

    session: UploadSession,
}

impl BlobWriter {
    pub async fn write(&mut self, content_length: u64, body: Body) -> Result<u64> {
        let chunk = self
            .objects
            .clone()
            .upload_chunk(&mut self.session, content_length, body)
            .await?;

        self.metadata.insert_chunk(&self.session, &chunk).await?;
        self.metadata.update_session(&self.session).await?;

        // TODO: return uploaded content length here
        Ok(0)
    }
}
