use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use hyper::body::Body;
use uuid::Uuid;

use portfolio_core::registry::BoxedUploadSession;
use portfolio_core::registry::{BlobStore, BlobWriter};
use portfolio_core::registry::{BoxedBlob, BoxedBlobWriter};
use portfolio_core::Error as CoreError;
use portfolio_core::Result;
use portfolio_core::{ChunkedBody, DigestBody, Digester, OciDigest};
use portfolio_objectstore::{Chunk, Key, ObjectStore};

use super::errors::Error;
use super::metadata::{
    Chunk as MetadataChunk, PostgresMetadataPool, PostgresMetadataTx, UploadSession,
};

pub struct PgBlobStore {
    pub(crate) metadata: PostgresMetadataPool,
    pub(crate) objects: Arc<dyn ObjectStore>,
}

impl PgBlobStore {
    pub fn new(metadata: PostgresMetadataPool, objects: Arc<dyn ObjectStore>) -> Self {
        Self {
            metadata,
            objects: objects,
        }
    }
}

type TryBytes = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
impl BlobStore for PgBlobStore {
    async fn resume(
        &self,
        session_uuid: &Uuid,
        start_of_range: Option<u64>,
    ) -> Result<BoxedBlobWriter> {
        // retrieve the session or fail if it doesn't exist
        let mut session = self
            .metadata
            .get_conn()
            .await?
            .get_session(session_uuid)
            .await
            .map_err(|_| CoreError::BlobUploadInvalid(None))?;

        if let Some(start) = start_of_range {
            if !session.validate_range(start) {
                tracing::debug!("content range start {start} is invalid");
                return Err(CoreError::BlobUploadInvalid(Some(
                    "content range start is invalid".to_string(),
                ))
                .into());
            }
        }

        if session.upload_id.is_none() {
            session.upload_id = Some(
                self.objects
                    .initiate_chunked_upload(&Key::from(&session.uuid))
                    .await
                    .map_err(Error::from)?,
            );
        }

        Ok(Box::new(PgBlobWriter {
            metadata: self.metadata.clone(),
            objects: self.objects.clone(),
            session: Some(session),
        }))
    }

    async fn put(&self, digest: &OciDigest, content_length: u64, body: Body) -> Result<Uuid> {
        let mut tx = self.metadata.get_tx().await?;
        let uuid = match tx.get_blob(digest).await? {
            Some(b) => {
                // verify blob actually exists before returning a potentially bogus uuid
                if self
                    .objects
                    .exists(&Key::from(&b.id))
                    .await
                    .map_err(Error::from)?
                {
                    return Ok(b.id);
                }
                b.id
            }
            None => tx
                .insert_blob(digest, content_length as i64)
                .await
                .map_err(Error::from)?,
        };

        // upload blob
        let digester = Arc::new(Mutex::new(digest.digester()));
        let stream_body = DigestBody::from_body(body, digester);
        self.objects
            .put(&Key::from(&uuid), stream_body.into(), content_length)
            .await
            .map_err(Error::from)?;

        // TODO: validate digest
        // TODO: validate content length

        tx.commit().await.map_err(Error::from)?;

        Ok(uuid)
    }

    async fn get(
        &self,
        key: &OciDigest,
    ) -> Result<Option<(BoxedBlob, BoxStream<'static, TryBytes>)>> {
        if let Some(blob) = self.metadata.get_conn().await?.get_blob(key).await? {
            let body = self
                .objects
                .get(&Key::from(&blob.id))
                .await
                .map_err(Error::from)?;
            Ok(Some((Box::new(blob), body.map_err(|e| e.into()).boxed())))
        } else {
            Ok(None)
        }
    }

    async fn head(&self, key: &OciDigest) -> Result<Option<BoxedBlob>> {
        match self.metadata.get_conn().await?.get_blob(key).await? {
            Some(b) => Ok(Some(Box::new(b))),
            None => Ok(None),
        }
    }

    async fn delete(&self, digest: &OciDigest) -> Result<()> {
        let mut tx = self.metadata.get_tx().await?;

        let blob = tx
            .get_blob(digest)
            .await?
            .ok_or(CoreError::BlobUnknown(None))?;

        // TODO: handle the case where the blob is referenced
        tx.delete_blob(&blob.id).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub struct PgBlobWriter {
    metadata: PostgresMetadataPool,
    objects: Arc<dyn ObjectStore>,

    session: Option<UploadSession>,
}

impl PgBlobWriter {
    async fn write_chunk(
        &self,
        tx: &mut PostgresMetadataTx<'_>,
        session: &mut UploadSession,
        bytes: Bytes,
    ) -> Result<()> {
        let chunk = self
            .objects
            .upload_chunk(
                &session
                    .upload_id
                    .as_ref()
                    .expect("UploadSession.upload_id should always be Some here")
                    .as_str(),
                &Key::from(&session.uuid),
                session.chunk_number,
                bytes.len() as u64,
                bytes.into(),
            )
            .await
            .map_err(Error::from)?;

        tx.insert_chunk(&session, &MetadataChunk::from(chunk))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl BlobWriter for PgBlobWriter {
    async fn write(&mut self, content_length: u64, body: Body) -> Result<BoxedUploadSession> {
        let mut session = if let Some(session) = self.session.take() {
            session
        } else {
            return Err(CoreError::BlobWriterFinished);
        };
        tracing::debug!("before chunk upload: {:?}", session);
        let digester = Arc::new(Mutex::new(Digester::default()));
        let stream_body = DigestBody::from_body(body, digester.clone());
        let chunk = self
            .objects
            .upload_chunk(
                &session
                    .upload_id
                    .as_ref()
                    .expect("UploadSession.upload_id should always be Some here")
                    .as_str(),
                &Key::from(&session.uuid),
                session.chunk_number,
                content_length,
                stream_body.into(),
            )
            .await
            .map_err(Error::from)?;

        let mut conn = self.metadata.get_conn().await?;
        conn.insert_chunk(&session, &MetadataChunk::from(chunk))
            .await?;

        let digester = Arc::into_inner(digester)
            .expect("no other references should exist at this point")
            .into_inner()
            .expect("the mutex cannot be locked if there are no other Arc references");

        session.chunk_number += 1;
        session.last_range_end += digester.bytes() as i64 - 1;

        conn.update_session(&session).await?;

        // TODO: return uploaded content length here
        Ok(Box::new(session))
    }

    async fn write_chunked(&mut self, body: Body) -> Result<BoxedUploadSession> {
        let mut session = if let Some(session) = self.session.take() {
            session
        } else {
            return Err(CoreError::BlobWriterFinished);
        };
        let md = self.metadata.clone();
        let mut tx = md.get_tx().await?;
        let mut digester = Digester::default();

        let chunked = ChunkedBody::from_body(body);
        tokio::pin!(chunked);

        while let Some(vbytes) = chunked.next().await {
            for bytes in vbytes.into_iter() {
                digester.update(&bytes);
                self.write_chunk(&mut tx, &mut session, bytes).await?;
                session.chunk_number += 1;
            }
        }

        session.last_range_end += digester.bytes() as i64 - 1;
        tx.update_session(&session).await?;

        tx.commit().await?;
        Ok(Box::new(session))
    }

    async fn finalize(&mut self, digest: &OciDigest) -> Result<BoxedUploadSession> {
        let session = if let Some(session) = self.session.take() {
            session
        } else {
            return Err(CoreError::BlobWriterFinished);
        };
        // TODO: validate digest
        let mut tx = self.metadata.get_tx().await?;
        let uuid = match tx.get_blob(&digest).await? {
            Some(b) => b.id,
            None => tx.insert_blob(&digest, &session.last_range_end + 1).await?,
        };

        let blob_key = Key::from(&uuid);
        let session_key = Key::from(&session.uuid);

        if !self.objects.exists(&blob_key).await.map_err(Error::from)? {
            let chunks = tx
                .get_chunks(&session)
                .await?
                .into_iter()
                .map(Chunk::from)
                .collect();
            self.objects
                .finalize_chunked_upload(
                    session
                        .upload_id
                        .as_ref()
                        .expect("UploadSession.upload_id should always be Some here")
                        .as_str(),
                    &session_key,
                    chunks,
                    &blob_key,
                )
                .await
                .map_err(Error::from)?;
        } else {
            self.objects
                .abort_chunked_upload(
                    session
                        .upload_id
                        .as_ref()
                        .expect("UploadSession.upload_id should always be Some here")
                        .as_str(),
                    &session_key,
                )
                .await
                .map_err(Error::from)?;
        }

        tx.commit().await?;
        Ok(Box::new(session))
    }
}
