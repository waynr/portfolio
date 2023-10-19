use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use hyper::body::Body;
use uuid::Uuid;

pub struct Chunk {
    pub e_tag: Option<String>,
    pub chunk_number: i32,
}

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
        session_uuid: &Uuid,
    ) -> std::result::Result<String, Self::Error>;

    async fn upload_chunk(
        &self,
        upload_id: &str,
        session_uuid: &Uuid,
        chunk_number: i32,
        content_length: u64,
        body: Body,
    ) -> std::result::Result<Chunk, Self::Error>;

    async fn finalize_chunked_upload(
        &self,
        upload_id: &str,
        session_uuid: &Uuid,
        chunks: Vec<Chunk>,
        key: &Uuid,
    ) -> std::result::Result<(), Self::Error>;

    async fn abort_chunked_upload(
        &self,
        upload_id: &str,
        session_uuid: &Uuid,
    ) -> std::result::Result<(), Self::Error>;
}
