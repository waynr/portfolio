use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use hyper::body::Body;
use uuid::Uuid;

use portfolio_core::registry::{BlobStore, BlobWriter};
use portfolio_core::DistributionErrorCode;
use portfolio_core::{ChunkedBody, Digester, OciDigest, StreamObjectBody};
use portfolio_objectstore::{Chunk, ObjectStore, S3};

use super::errors::{Error, Result};
use super::metadata::{
    Blob, Chunk as MetadataChunk, PostgresMetadataPool, PostgresMetadataTx, UploadSession,
};

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
    type Error = Error;
    type UploadSession = UploadSession;
    type Blob = Blob;
    type BlobBody =
        BoxStream<'static, std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync>>>;

    async fn resume(
        &self,
        session_uuid: &Uuid,
        start_of_range: Option<u64>,
    ) -> Result<PgS3BlobWriter> {
        // retrieve the session or fail if it doesn't exist
        let mut session = self
            .metadata
            .get_conn()
            .await?
            .get_session(session_uuid)
            .await
            .map_err(|_| Error::DistributionSpecError(DistributionErrorCode::BlobUploadUnknown))?;

        if let Some(start) = start_of_range {
            if !session.validate_range(start) {
                tracing::debug!("content range start {start} is invalid");
                return Err(Error::DistributionSpecError(
                    DistributionErrorCode::BlobUploadInvalid,
                ));
            }
        }

        if session.upload_id.is_none() {
            session.upload_id = Some(self.objects.initiate_chunked_upload(&session.uuid).await?);
        }

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

    async fn get(&self, key: &OciDigest) -> Result<Option<(Self::Blob, Self::BlobBody)>> {
        if let Some(blob) = self.metadata.get_conn().await?.get_blob(key).await? {
            let body = self.objects.get_blob(&blob.id).await?;
            Ok(Some((blob, body.map_err(|e| e.into()).boxed())))
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
                portfolio_core::DistributionErrorCode::BlobUnknown,
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
            .upload_chunk(
                &self
                    .session
                    .upload_id
                    .as_ref()
                    .expect("UploadSession.upload_id should always be Some here")
                    .as_str(),
                &self.session.uuid,
                self.session.chunk_number,
                bytes.len() as u64,
                bytes.into(),
            )
            .await?;

        tx.insert_chunk(&self.session, &MetadataChunk::from(chunk))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl BlobWriter for PgS3BlobWriter {
    type Error = Error;
    type UploadSession = UploadSession;

    async fn write(mut self, content_length: u64, body: Body) -> Result<Self::UploadSession> {
        tracing::debug!("before chunk upload: {:?}", self.session);
        let digester = Arc::new(Mutex::new(Digester::default()));
        let stream_body = StreamObjectBody::from_body(body, digester.clone());
        let chunk = self
            .objects
            .upload_chunk(
                &self
                    .session
                    .upload_id
                    .as_ref()
                    .expect("UploadSession.upload_id should always be Some here")
                    .as_str(),
                &self.session.uuid,
                self.session.chunk_number,
                content_length,
                stream_body.into(),
            )
            .await?;

        let mut conn = self.metadata.get_conn().await?;
        conn.insert_chunk(&self.session, &MetadataChunk::from(chunk))
            .await?;

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

    async fn write_chunked(mut self, body: Body) -> Result<Self::UploadSession> {
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

    async fn finalize(self, digest: &OciDigest) -> Result<Self::UploadSession> {
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
            let chunks = tx
                .get_chunks(&self.session)
                .await?
                .into_iter()
                .map(Chunk::from)
                .collect();
            self.objects
                .finalize_chunked_upload(
                    self.session
                        .upload_id
                        .as_ref()
                        .expect("UploadSession.upload_id should always be Some here")
                        .as_str(),
                    &self.session.uuid,
                    chunks,
                    &uuid,
                )
                .await?;
        } else {
            self.objects
                .abort_chunked_upload(
                    self.session
                        .upload_id
                        .as_ref()
                        .expect("UploadSession.upload_id should always be Some here")
                        .as_str(),
                    &self.session.uuid,
                )
                .await?;
        }

        tx.commit().await?;
        Ok(self.session)
    }
}
