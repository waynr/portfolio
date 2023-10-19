pub mod types;
pub use types::{ManifestRef, ManifestSpec, TagsList};

use async_trait::async_trait;
use axum::body::Bytes;
use hyper::body::Body;
use oci_spec::image::ImageIndex;
use oci_spec::image::MediaType;
use uuid::Uuid;

use crate::oci_digest::OciDigest;

#[async_trait]
pub trait RepositoryStoreManager: Clone + Send + Sync + 'static {
    type RepositoryStore: RepositoryStore;
    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;

    async fn get(
        &self,
        name: &str,
    ) -> std::result::Result<Option<Self::RepositoryStore>, Self::Error>;
    async fn create(&self, name: &str) -> std::result::Result<Self::RepositoryStore, Self::Error>;
}

#[async_trait]
pub trait RepositoryStore: Clone + Send + Sync + 'static {
    type ManifestStore: ManifestStore;
    type BlobStore: BlobStore;
    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;
    type UploadSession: UploadSession + Send + Sync + 'static;

    fn name(&self) -> &str;
    fn get_manifest_store(&self) -> Self::ManifestStore;
    fn get_blob_store(&self) -> Self::BlobStore;

    async fn get_tags(
        &self,
        n: Option<i64>,
        last: Option<String>,
    ) -> std::result::Result<TagsList, Self::Error>;

    async fn new_upload_session(&self) -> std::result::Result<Self::UploadSession, Self::Error>;

    async fn get_upload_session(
        &self,
        session_uuid: &Uuid,
    ) -> std::result::Result<Self::UploadSession, Self::Error>;

    async fn delete_session(&self, session_uuid: &Uuid) -> std::result::Result<(), Self::Error>;
}

#[async_trait]
pub trait ManifestStore: Send + Sync + 'static {
    type Manifest: Manifest;
    type ManifestBodyStreamError: std::error::Error + Send + Sync + 'static;
    type ManifestBody: futures_core::stream::Stream<
            Item = std::result::Result<Bytes, Self::ManifestBodyStreamError>,
        > + Send
        + Sync
        + 'static;
    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;

    async fn head(
        &self,
        key: &ManifestRef,
    ) -> std::result::Result<Option<Self::Manifest>, Self::Error>;

    async fn get(
        &self,
        key: &ManifestRef,
    ) -> std::result::Result<Option<(Self::Manifest, Self::ManifestBody)>, Self::Error>;

    async fn put(
        &mut self,
        key: &ManifestRef,
        spec: &ManifestSpec,
        bytes: Bytes,
    ) -> std::result::Result<OciDigest, Self::Error>;

    async fn delete(&mut self, key: &ManifestRef) -> std::result::Result<(), Self::Error>;

    async fn get_referrers(
        &self,
        subject: &OciDigest,
        artifact_type: Option<String>,
    ) -> std::result::Result<ImageIndex, Self::Error>;
}

#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    type BlobWriter: BlobWriter;
    type Blob: Blob;
    type BlobBodyStreamError: std::error::Error + Send + Sync + 'static;
    type BlobBody: futures_core::stream::Stream<Item = std::result::Result<Bytes, Self::BlobBodyStreamError>>
        + Send
        + Sync
        + 'static;
    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;
    type UploadSession: UploadSession + Send + Sync + 'static;

    async fn head(&self, key: &OciDigest) -> std::result::Result<Option<Self::Blob>, Self::Error>;

    async fn get(
        &self,
        key: &OciDigest,
    ) -> std::result::Result<Option<(Self::Blob, Self::BlobBody)>, Self::Error>;

    async fn put(
        &mut self,
        digest: &OciDigest,
        content_length: u64,
        body: Body,
    ) -> std::result::Result<Uuid, Self::Error>;

    async fn delete(&mut self, digest: &OciDigest) -> std::result::Result<(), Self::Error>;

    async fn resume(
        &self,
        session_uuid: &Uuid,
        start: Option<u64>,
    ) -> std::result::Result<Self::BlobWriter, Self::Error>;
}

#[async_trait]
pub trait BlobWriter: Send + Sync + 'static {
    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;
    type UploadSession: UploadSession + Send + Sync + 'static;

    async fn write(
        self,
        content_length: u64,
        body: Body,
    ) -> std::result::Result<Self::UploadSession, Self::Error>;

    async fn write_chunked(
        self,
        body: Body,
    ) -> std::result::Result<Self::UploadSession, Self::Error>;

    async fn finalize(
        self,
        digest: &OciDigest,
    ) -> std::result::Result<Self::UploadSession, Self::Error>;
}

pub trait Blob {
    fn bytes_on_disk(&self) -> u64;
}

pub trait Manifest {
    fn bytes_on_disk(&self) -> u64;
    fn digest(&self) -> &OciDigest;
    fn media_type(&self) -> &Option<MediaType>;
}

pub trait UploadSession {
    fn uuid(&self) -> &Uuid;
    fn upload_id(&self) -> &Option<String>;
    fn last_range_end(&self) -> i64;
}
