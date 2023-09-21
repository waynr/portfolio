use aws_sdk_s3::types::ByteStream;
use axum::body::StreamBody;
use hyper::body::Body;

use crate::{
    errors::Result,
    metadata::{PostgresMetadata, Registry},
    objects::ObjectStore,
    objects::StreamObjectBody,
    registry::UploadSession,
    OciDigest,
};

pub struct BlobStore<'b, O>
where
    O: ObjectStore,
{
    metadata: PostgresMetadata,
    objects: O,
    registry: &'b Registry,
}

impl<'b, O> BlobStore<'b, O>
where
    O: ObjectStore,
{
    pub fn new(metadata: PostgresMetadata, objects: O, registry: &'b Registry) -> Self {
        Self {
            metadata,
            objects,
            registry,
        }
    }

    pub async fn resume(&self, session: &'b mut UploadSession) -> Result<BlobWriter<'b, O>> {
        match session.upload_id {
            Some(_) => (),
            None => self.objects.initiate_chunked_upload(session).await?,
        };

        Ok(BlobWriter {
            metadata: self.metadata.clone(),
            objects: self.objects.clone(),
            session,
            registry: self.registry,
        })
    }

    pub async fn upload(&mut self, digest: &str, content_length: u64, body: Body) -> Result<()> {
        let oci_digest: OciDigest = digest.try_into()?;

        // upload blob
        let digester = oci_digest.digester();
        let stream_body = StreamObjectBody::from_body(body, digester);
        self.objects
            .upload_blob(&oci_digest, stream_body.into(), content_length)
            .await?;

        // TODO: validate digest
        // TODO: validate content length

        // insert metadata
        if !self
            .metadata
            .blob_exists(&self.registry.id, &oci_digest)
            .await?
        {
            self.metadata
                .insert_blob(&self.registry.id, &oci_digest)
                .await?;
        }

        Ok(())
    }

    pub async fn get_blob(&self, key: &OciDigest) -> Result<Option<StreamBody<ByteStream>>> {
        if let Some(blob) = self.metadata.get_blob(&self.registry.id, key).await? {
            Ok(Some(
                self.objects
                    .get_blob(&blob.digest.as_str().try_into()?)
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }
}

pub struct BlobWriter<'a, O: ObjectStore> {
    metadata: PostgresMetadata,
    objects: O,

    registry: &'a Registry,
    session: &'a mut UploadSession,
}

impl<'a, O> BlobWriter<'a, O>
where
    O: ObjectStore,
{
    pub async fn write(&mut self, content_length: u64, body: Body) -> Result<u64> {
        let chunk = self
            .objects
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
