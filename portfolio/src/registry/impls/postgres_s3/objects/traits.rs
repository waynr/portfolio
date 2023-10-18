use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use hyper::body::Body;
use uuid::Uuid;

use crate::registry::{Chunk, UploadSession};

#[async_trait]
pub trait ObjectStore: Clone + Send + Sync + 'static {
    type Error: std::error::Error;

    async fn get_blob(&self, key: &Uuid) -> std::result::Result<ByteStream, Self::Error>;

    async fn blob_exists(&self, key: &Uuid) -> std::result::Result<bool, Self::Error>;

    async fn upload_blob(
        &self,
        key: &Uuid,
        body: Body,
        content_length: u64,
    ) -> std::result::Result<(), Self::Error>;

    async fn delete_blob(&self, key: &Uuid) -> std::result::Result<(), Self::Error>;

    async fn initiate_chunked_upload(
        &self,
        session: &mut UploadSession,
    ) -> std::result::Result<(), Self::Error>;

    async fn upload_chunk(
        &self,
        session: &UploadSession,
        content_length: u64,
        body: Body,
    ) -> std::result::Result<Chunk, Self::Error>;

    async fn finalize_chunked_upload(
        &self,
        session: &UploadSession,
        chunks: Vec<Chunk>,
        key: &Uuid,
    ) -> std::result::Result<(), Self::Error>;

    async fn abort_chunked_upload(
        &self,
        session: &UploadSession,
    ) -> std::result::Result<(), Self::Error>;
}
