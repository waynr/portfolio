pub mod repositories;
pub use repositories::Repository;

pub mod blobs;
pub use blobs::BlobStore;

pub mod manifests;
pub use manifests::ManifestStore;

pub mod metadata;
pub mod objects;

pub mod types;
pub use types::{
    Blob, Manifest, ManifestRef, ManifestSpec, Repository as RepositoryMetadata, Tag, TagsList,
};

pub mod session;
pub use session::{Chunk, Chunks};
pub use session::{UploadSession, UploadSessions};

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use axum::body::Bytes;
use hyper::body::Body;
use oci_spec::image::ImageIndex;
use uuid::Uuid;

use crate::{errors::Result, oci_digest::OciDigest};

#[async_trait]
pub trait RepositoryStoreTrait: Send + Sync + 'static {
    type ManifestStore: ManifestStoreTrait;
    type BlobStore: BlobStoreTrait;

    fn name(&self) -> &str;
    fn get_manifest_store(&self) -> Self::ManifestStore;
    fn get_blob_store(&self) -> Self::BlobStore;

    async fn get_tags(&self, n: Option<i64>, last: Option<String>) -> Result<TagsList>;

    async fn new_upload_session(&self) -> Result<UploadSession>;

    async fn get_upload_session(&self, session_uuid: &Uuid) -> Result<UploadSession>;

    async fn delete_session(&self, session: &UploadSession) -> Result<()>;

    async fn create_repository(&self, name: &String) -> Result<RepositoryMetadata>;
}

#[async_trait]
pub trait ManifestStoreTrait: Send + Sync + 'static {
    async fn head(&self, key: &ManifestRef) -> Result<Option<Manifest>>;

    async fn get(&self, key: &ManifestRef) -> Result<Option<(Manifest, ByteStream)>>;

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

#[async_trait]
pub trait BlobStoreTrait: Send + Sync + 'static {
    type BlobWriter: BlobWriterTrait;

    async fn head(&self, key: &OciDigest) -> Result<Option<Blob>>;

    async fn get(&self, key: &OciDigest) -> Result<Option<(Blob, ByteStream)>>;

    async fn put(&mut self, digest: &OciDigest, content_length: u64, body: Body) -> Result<Uuid>;

    async fn delete(&mut self, digest: &OciDigest) -> Result<()>;

    async fn resume(&self, session: &mut UploadSession) -> Result<Self::BlobWriter>;
}

#[async_trait]
pub trait BlobWriterTrait: Send + Sync + 'static {
    async fn write(&mut self, content_length: u64, body: Body) -> Result<()>;

    async fn write_chunked(&mut self, body: Body) -> Result<()>;

    async fn finalize(&mut self, digest: &OciDigest) -> Result<()>;
}
