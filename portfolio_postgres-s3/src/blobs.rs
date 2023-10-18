use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use hyper::body::Body;
use tokio_stream::StreamExt;
use uuid::Uuid;

use portfolio::registry::{BlobStore, BlobWriter, UploadSession};
use portfolio::{ChunkedBody, StreamObjectBody, Digester, OciDigest};

use super::metadata::{Blob, PostgresMetadataPool, PostgresMetadataTx};
use super::objects::{ObjectStore, S3};
use super::errors::{Error, Result};

pub struct PgS3BlobStore {
    pub(crate) metadata: PostgresMetadataPool,
    pub(crate) objects: S3,
}

impl PgS3BlobStore {
    pub fn new(metadata: PostgresMetadataPool, objects: S3) -> Self {
        Self { metadata, objects }
    }
}

#[async_trait]
impl BlobStore for PgS3BlobStore {
    type BlobWriter = PgS3BlobWriter;
    type Blob = Blob;
    type Error = Error;

    async fn resume(&self, mut session: UploadSession) -> Result<PgS3BlobWriter> {
        match session.upload_id {
            Some(_) => (),
            None => self.objects.initiate_chunked_upload(&mut session).await?,
        };

        Ok(PgS3BlobWriter {
            metadata: self.metadata.clone(),
            objects: self.objects.clone(),
            session,
        })
    }

    async fn put(&mut self, digest: &OciDigest, content_length: u64, body: Body) -> Result<Uuid> {
        let mut tx = self.metadata.get_tx().await?;
        let uuid = match tx.get_blob(digest).await? {
            Some(b) => {
                // verify blob actually exists before returning a potentially bogus uuid
                if self.objects.blob_exists(&b.id).await? {
                    return Ok(b.id);
                }
                b.id
            }
            None => tx.insert_blob(digest, content_length as i64).await?,
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

    async fn get(&self, key: &OciDigest) -> Result<Option<(Blob, ByteStream)>> {
        if let Some(blob) = self.metadata.get_conn().await?.get_blob(key).await? {
            let body = self.objects.get_blob(&blob.id).await?;
            Ok(Some((blob, body)))
        } else {
            Ok(None)
        }
    }

    async fn head(&self, key: &OciDigest) -> Result<Option<Blob>> {
        self.metadata.get_conn().await?.get_blob(key).await
    }

    async fn delete(&mut self, digest: &OciDigest) -> Result<()> {
        let mut tx = self.metadata.get_tx().await?;

        let blob = tx
            .get_blob(digest)
            .await?
            .ok_or(Error::DistributionSpecError(
                portfolio::DistributionErrorCode::BlobUnknown,
            ))?;

        // TODO: handle the case where the blob is referenced
        tx.delete_blob(&blob.id).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub struct PgS3BlobWriter {
    metadata: PostgresMetadataPool,
    objects: S3,

    session: UploadSession,
}

impl PgS3BlobWriter {
    async fn write_chunk(&mut self, tx: &mut PostgresMetadataTx<'_>, bytes: Bytes) -> Result<()> {
        let chunk = self
            .objects
            .upload_chunk(&self.session, bytes.len() as u64, bytes.into())
            .await?;

        tx.insert_chunk(&self.session, &chunk).await?;
        Ok(())
    }
}

#[async_trait]
impl BlobWriter for PgS3BlobWriter {
    type Error = Error;

    async fn write(mut self, content_length: u64, body: Body) -> Result<UploadSession> {
        tracing::debug!("before chunk upload: {:?}", self.session);
        let digester = Arc::new(Mutex::new(Digester::default()));
        let stream_body = StreamObjectBody::from_body(body, digester.clone());
        let chunk = self
            .objects
            .upload_chunk(&self.session, content_length, stream_body.into())
            .await?;

        let mut conn = self.metadata.get_conn().await?;
        conn.insert_chunk(&self.session, &chunk).await?;

        let digester = Arc::into_inner(digester)
            .expect("no other references should exist at this point")
            .into_inner()
            .expect("the mutex cannot be locked if there are no other Arc references");

        self.session.chunk_number += 1;
        self.session.last_range_end += digester.bytes() as i64 - 1;

        conn.update_session(&self.session).await?;

        // TODO: return uploaded content length here
        Ok(self.session)
    }

    async fn write_chunked(mut self, body: Body) -> Result<UploadSession> {
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
        tx.update_session(&self.session).await?;

        tx.commit().await?;
        Ok(self.session)
    }

    async fn finalize(self, digest: &OciDigest) -> Result<UploadSession> {
        // TODO: validate digest
        let mut tx = self.metadata.get_tx().await?;
        let uuid = match tx.get_blob(&digest).await? {
            Some(b) => b.id,
            None => {
                tx.insert_blob(&digest, &self.session.last_range_end + 1)
                    .await?
            }
        };

        if !self.objects.blob_exists(&uuid).await? {
            let chunks = tx.get_chunks(&self.session).await?;
            self.objects
                .finalize_chunked_upload(&self.session, chunks, &uuid)
                .await?;
        } else {
            self.objects.abort_chunked_upload(&self.session).await?;
        }

        tx.commit().await?;
        Ok(self.session)
    }
}
