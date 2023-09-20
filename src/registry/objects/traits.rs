use axum::body::StreamBody;
use aws_sdk_s3::types::ByteStream;
use async_trait::async_trait;
use hyper::body::Body;

use crate::OciDigest;
use crate::Result;
use crate::registry::UploadSession;
use crate::registry::Chunk;

#[async_trait]
pub trait ObjectStore: Clone + Send + Sync + 'static {
    async fn get_blob(&self, key: &OciDigest) -> Result<StreamBody<ByteStream>>;

    async fn blob_exists(&self, key: &OciDigest) -> Result<bool>;

    async fn upload_blob(&self, key: &OciDigest, body: Body, content_length: u64) -> Result<()>;

    async fn initiate_chunked_upload(&self, session: &mut UploadSession) -> Result<()>;

    async fn upload_chunk(&self, session: &mut UploadSession, content_length: u64, body: Body) -> Result<Chunk>;

    async fn finalize_chunked_upload(&self, session: &UploadSession, chunks: Vec<Chunk>, dgst: &OciDigest) -> Result<()>;

    async fn abort_chunked_upload(&self, session: &UploadSession) -> Result<()>;
}

