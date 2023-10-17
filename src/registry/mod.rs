pub(crate) mod impls;

pub mod types;
pub use types::{
    ManifestRef, ManifestSpec, TagsList,
};

pub mod session;
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use axum::body::Bytes;
use hyper::body::Body;
use oci_spec::image::ImageIndex;
use oci_spec::image::MediaType;
pub use session::{Chunk, Chunks, UploadSession, UploadSessions};
use uuid::Uuid;

use crate::errors::Result;
use crate::oci_digest::OciDigest;
pub use crate::registry::impls::postgres_s3::config::PgS3RepositoryFactory;
pub use crate::registry::impls::postgres_s3::repositories::PgS3Repository;

#[async_trait]
pub trait RepositoryStoreManager: Clone + Send + Sync + 'static {
    type RepositoryStore: RepositoryStore;

    async fn get(&self, name: &str) -> Result<Option<Self::RepositoryStore>>;
    async fn create(&self, name: &str) -> Result<Self::RepositoryStore>;
}

#[async_trait]
pub trait RepositoryStore: Clone + Send + Sync + 'static {
    type ManifestStore: ManifestStore;
    type BlobStore: BlobStore;

    fn name(&self) -> &str;
    fn get_manifest_store(&self) -> Self::ManifestStore;
    fn get_blob_store(&self) -> Self::BlobStore;

    async fn get_tags(&self, n: Option<i64>, last: Option<String>) -> Result<TagsList>;

    async fn new_upload_session(&self) -> Result<UploadSession>;

    async fn get_upload_session(&self, session_uuid: &Uuid) -> Result<UploadSession>;

    async fn delete_session(&self, session: &UploadSession) -> Result<()>;
}

pub trait Manifest {
    fn bytes_on_disk(&self) -> u64;
    fn digest(&self) -> &OciDigest;
    fn media_type(&self) -> &Option<MediaType>;
}

#[async_trait]
pub trait ManifestStore: Send + Sync + 'static {
    type Manifest: Manifest;

    async fn head(&self, key: &ManifestRef) -> Result<Option<Self::Manifest>>;

    async fn get(&self, key: &ManifestRef) -> Result<Option<(Self::Manifest, ByteStream)>>;

    async fn put(
        &mut self,
        key: &ManifestRef,
        spec: &ManifestSpec,
        bytes: Bytes,
    ) -> Result<OciDigest>;

    async fn delete(&mut self, key: &ManifestRef) -> Result<()>;

    async fn get_referrers(
        &self,
        subject: &OciDigest,
        artifact_type: Option<String>,
    ) -> Result<ImageIndex>;
}

pub trait Blob {
    fn bytes_on_disk(&self) -> u64;
}

#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    type BlobWriter: BlobWriter;
    type Blob: Blob;

    async fn head(&self, key: &OciDigest) -> Result<Option<Self::Blob>>;

    async fn get(&self, key: &OciDigest) -> Result<Option<(Self::Blob, ByteStream)>>;

    async fn put(&mut self, digest: &OciDigest, content_length: u64, body: Body) -> Result<Uuid>;

    async fn delete(&mut self, digest: &OciDigest) -> Result<()>;

    async fn resume(&self, session: UploadSession) -> Result<Self::BlobWriter>;
}

#[async_trait]
pub trait BlobWriter: Send + Sync + 'static {
    async fn write(self, content_length: u64, body: Body) -> Result<UploadSession>;

    async fn write_chunked(self, body: Body) -> Result<UploadSession>;

    async fn finalize(self, digest: &OciDigest) -> Result<UploadSession>;
}
