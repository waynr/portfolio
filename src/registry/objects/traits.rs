use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use axum::body::StreamBody;
use hyper::body::Body;
use uuid::Uuid;

use crate::registry::Chunk;
use crate::registry::UploadSession;
use crate::Result;

#[async_trait]
pub trait ObjectStore: Clone + Send + Sync + 'static {
    async fn get_blob(&self, key: &Uuid) -> Result<StreamBody<ByteStream>>;

    async fn blob_exists(&self, key: &Uuid) -> Result<bool>;

    async fn upload_blob(&self, key: &Uuid, body: Body, content_length: u64) -> Result<()>;

    async fn delete_blob(&self, key: &Uuid) -> Result<()>;

    async fn initiate_chunked_upload(&self, session: &mut UploadSession) -> Result<()>;

    async fn upload_chunk(
        &self,
        session: &mut UploadSession,
        content_length: u64,
        body: Body,
    ) -> Result<Chunk>;

    async fn finalize_chunked_upload(
        &self,
        session: &UploadSession,
        chunks: Vec<Chunk>,
        key: &Uuid,
    ) -> Result<()>;

    async fn abort_chunked_upload(&self, session: &UploadSession) -> Result<()>;
}
