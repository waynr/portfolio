use aws_sdk_s3::primitives::ByteStream;
use axum::body::StreamBody;
use hyper::body::Body;
use uuid::Uuid;

use crate::{
    errors::Result,
    metadata::{PostgresMetadataPool, Registry},
    objects::ObjectStore,
    objects::StreamObjectBody,
    registry::UploadSession,
    OciDigest,
};

pub struct BlobStore<'b, O>
where
    O: ObjectStore,
{
    metadata: PostgresMetadataPool,
    objects: O,
    registry: &'b Registry,
}

impl<'b, O> BlobStore<'b, O>
where
    O: ObjectStore,
{
    pub fn new(metadata: PostgresMetadataPool, objects: O, registry: &'b Registry) -> Self {
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

    pub async fn upload(&mut self, digest: &OciDigest, content_length: u64, body: Body) -> Result<Uuid> {
        // TODO: replace this connection with a transaction
        let mut conn = self.metadata.get_conn().await?;
        let uuid = match conn.get_blob(&self.registry.id, digest).await? {
            Some(b) => {
                // verify blob actually exists before returning a potentially bogus uuid
                if b.uploaded && self.objects.blob_exists(&b.id).await? {
                    return Ok(b.id);
                }
                b.id
            }
            None => {
                conn.insert_blob(&self.registry.id, digest, false)
                    .await?
            }
        };

        // upload blob
        let digester = digest.digester();
        let stream_body = StreamObjectBody::from_body(body, digester);
        self.objects
            .upload_blob(&uuid, stream_body.into(), content_length)
            .await?;

        // TODO: validate digest
        // TODO: validate content length

        conn.update_blob(&uuid, true).await?;

        Ok(uuid)
    }

    pub async fn get_blob(&self, key: &OciDigest) -> Result<Option<StreamBody<ByteStream>>> {
        if let Some(blob) = self
            .metadata
            .get_conn()
            .await?
            .get_blob(&self.registry.id, key)
            .await?
        {
            Ok(Some(self.objects.get_blob(&blob.id).await?))
        } else {
            Ok(None)
        }
    }

    pub async fn blob_exists(&self, key: &OciDigest) -> Result<bool> {
        self.metadata
            .get_conn()
            .await?
            .blob_exists(&self.registry.id, key)
            .await
    }
}

pub struct BlobWriter<'a, O: ObjectStore> {
    metadata: PostgresMetadataPool,
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

        let mut conn = self.metadata.get_conn().await?;
        conn.insert_chunk(self.session, &chunk).await?;
        conn.update_session(self.session).await?;

        // TODO: return uploaded content length here
        Ok(0)
    }

    pub async fn finalize(&mut self, digest: &OciDigest) -> Result<()> {
        let mut conn = self.metadata.get_conn().await?;
        let uuid = match conn.get_blob(&self.registry.id, &digest).await? {
            Some(b) => b.id,
            None => conn.insert_blob(&self.registry.id, &digest, false).await?,
        };

        if !self.objects.blob_exists(&uuid).await? {
            let chunks = conn.get_chunks(self.session).await?;
            self.objects
                .finalize_chunked_upload(self.session, chunks, &uuid)
                .await?;
        } else {
            self.objects.abort_chunked_upload(self.session).await?;
        }

        conn.update_blob(&uuid, true).await?;

        Ok(())
    }
}
