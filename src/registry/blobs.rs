use std::sync::Arc;

use hyper::body::Body;

use crate::{
    OciDigest,
    errors::Result,
    metadata::{PostgresMetadata, Registry},
    objects::S3,
    registry::UploadSession,
};

pub struct BlobStore<'b> {
    metadata: Arc<PostgresMetadata>,
    objects: Arc<S3>,
    registry: &'b Registry,
}

impl<'b> BlobStore<'b> {
    pub fn new(metadata: Arc<PostgresMetadata>, objects: Arc<S3>, registry: &'b Registry) -> Self {
        Self {
            metadata,
            objects,
            registry,
        }
    }

    pub async fn resume(&self, session: &'b mut UploadSession) -> Result<BlobWriter<'b>> {
        match session.upload_id {
            Some(_) => (),
            None => {
                self.objects
                    .clone()
                    .initiate_chunked_upload(session)
                    .await?
            }
        };

        Ok(BlobWriter {
            metadata: self.metadata.clone(),
            objects: self.objects.clone(),
            session,
            registry: self.registry,
        })
    }
}

pub struct BlobWriter<'a> {
    metadata: Arc<PostgresMetadata>,
    objects: Arc<S3>,

    registry: &'a Registry,
    session: &'a mut UploadSession,
}

impl<'a> BlobWriter<'a> {
    pub async fn write(&mut self, content_length: u64, body: Body) -> Result<u64> {
        let chunk = self
            .objects
            .clone()
            .upload_chunk(self.session, content_length, body)
            .await?;

        self.metadata.insert_chunk(self.session, &chunk).await?;
        self.metadata.update_session(self.session).await?;

        // TODO: return uploaded content length here
        Ok(0)
    }

    pub async fn finalize(&mut self, digest: &OciDigest) -> Result<()> {
        if !self.objects.blob_exists(digest).await? {
            let chunks = self.metadata.get_chunks(self.session).await?;
            self.objects
                .finalize_chunked_upload(self.session, chunks, digest)
                .await?;
        } else {
            self.objects.abort_chunked_upload(self.session).await?;
        }

        if !self.metadata.blob_exists(&self.registry.id, digest).await? {
            self.metadata.insert_blob(&self.registry.id, digest).await?;
        }
        Ok(())
    }
}
