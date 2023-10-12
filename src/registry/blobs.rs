use std::sync::{Arc, Mutex};

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use hyper::body::Body;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::{
    errors::{Error, Result},
    metadata::{PostgresMetadataPool, PostgresMetadataTx, Registry},
    objects::ChunkedBody,
    objects::ObjectStore,
    objects::StreamObjectBody,
    registry::UploadSession,
    Digester, DistributionErrorCode, OciDigest,
};

pub struct BlobStore<'b, O>
where
    O: ObjectStore,
{
    pub(crate) metadata: PostgresMetadataPool,
    pub(crate) objects: O,
    pub(crate) registry: &'b Registry,
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

    pub async fn upload(
        &mut self,
        digest: &OciDigest,
        content_length: u64,
        body: Body,
    ) -> Result<Uuid> {
        let mut tx = self.metadata.get_tx().await?;
        let uuid = match tx.get_blob(&self.registry.id, digest).await? {
            Some(b) => {
                // verify blob actually exists before returning a potentially bogus uuid
                if self.objects.blob_exists(&b.id).await? {
                    return Ok(b.id);
                }
                b.id
            }
            None => tx.insert_blob(&self.registry.id, digest).await?,
        };

        // upload blob
        let digester = Arc::new(Mutex::new(digest.digester()));
        let stream_body = StreamObjectBody::from_body(body, digester);
        self.objects
            .upload_blob(&uuid, stream_body.into(), content_length)
            .await?;

        // TODO: validate digest
        // TODO: validate content length

        tx.commit().await?;

        Ok(uuid)
    }

    pub async fn get_blob(&self, key: &OciDigest) -> Result<Option<ByteStream>> {
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

    pub async fn delete_blob(&mut self, digest: &OciDigest) -> Result<()> {
        let mut tx = self.metadata.get_tx().await?;

        let blob =
            tx.get_blob(&self.registry.id, digest)
                .await?
                .ok_or(Error::DistributionSpecError(
                    DistributionErrorCode::BlobUnknown,
                ))?;

        // TODO: handle the case where the blob is referenced
        tx.delete_blob(&blob.id).await?;
        tx.commit().await?;
        Ok(())
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
    pub async fn write(&mut self, content_length: u64, body: Body) -> Result<()> {
        tracing::debug!("before chunk upload: {:?}", self.session);
        let digester = Arc::new(Mutex::new(Digester::default()));
        let stream_body = StreamObjectBody::from_body(body, digester.clone());
        let chunk = self
            .objects
            .upload_chunk(self.session, content_length, stream_body.into())
            .await?;

        let mut conn = self.metadata.get_conn().await?;
        conn.insert_chunk(self.session, &chunk).await?;

        let digester = Arc::into_inner(digester)
            .expect("no other references should exist at this point")
            .into_inner()
            .expect("the mutex cannot be locked if there are no other Arc references");

        self.session.chunk_number += 1;
        self.session.last_range_end += digester.bytes() as i64 - 1;

        conn.update_session(self.session).await?;

        // TODO: return uploaded content length here
        Ok(())
    }

    pub async fn write_chunked(&mut self, body: Body) -> Result<()> {
        let md = self.metadata.clone();
        let mut tx = md.get_tx().await?;
        let mut digester = Digester::default();

        let chunked = ChunkedBody::from_body(body);
        tokio::pin!(chunked);

        while let Some(vbytes) = chunked.next().await {
            for bytes in vbytes.into_iter() {
                digester.update(&bytes);
                self.write_chunk(&mut tx, bytes).await?;
                self.session.chunk_number += 1;
            }
        }

        self.session.last_range_end += digester.bytes() as i64 - 1;
        tx.update_session(self.session).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn write_chunk(
        &mut self,
        tx: &mut PostgresMetadataTx<'_>,
        bytes: Bytes,
    ) -> Result<()> {
        let chunk = self
            .objects
            .upload_chunk(self.session, bytes.len() as u64, bytes.into())
            .await?;

        tx.insert_chunk(self.session, &chunk).await?;
        Ok(())
    }

    pub async fn finalize(&mut self, digest: &OciDigest) -> Result<()> {
        // TODO: validate digest
        let mut tx = self.metadata.get_tx().await?;
        let uuid = match tx.get_blob(&self.registry.id, &digest).await? {
            Some(b) => b.id,
            None => tx.insert_blob(&self.registry.id, &digest).await?,
        };

        if !self.objects.blob_exists(&uuid).await? {
            let chunks = tx.get_chunks(self.session).await?;
            self.objects
                .finalize_chunked_upload(self.session, chunks, &uuid)
                .await?;
        } else {
            self.objects.abort_chunked_upload(self.session).await?;
        }

        tx.commit().await
    }
}
