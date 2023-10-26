use std::collections::HashMap;

use async_trait::async_trait;
use axum::Json;
use bytes::Bytes;
use futures_core::Stream;
use hyper::body::Body;
use oci_spec::image::{Descriptor, ImageIndex, ImageManifest, MediaType};
use oci_spec::distribution::TagList;
use once_cell::sync::Lazy;
use regex::Regex;
use uuid::Uuid;

use crate::errors::{DistributionErrorCode, Error, Result};
use crate::oci_digest::OciDigest;

/// Create & get `Self::RepositoryStore` instances. Backend implementations may impose their own
/// access control and repository limit policies.
///
#[async_trait]
pub trait RepositoryStoreManager: Clone + Send + Sync + 'static {
    /// The `RepositoryStore` implementation an implementing type provides.
    type RepositoryStore: RepositoryStore;

    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;

    /// Get `RepositoryStore` corresponding to the given name, if it already exists. This name
    /// corresponds to the `<name>` in distribution-spec API endpoints like
    /// `/v2/<name>/blobs/<digest>`.
    async fn get(
        &self,
        name: &str,
    ) -> std::result::Result<Option<Self::RepositoryStore>, Self::Error>;

    /// Create new `RepositoryStore` with the given name. This name corresponds to the
    /// `<name>` in distribution-spec API endpoints like `/v2/<name>/blobs/<digest>`.
    async fn create(&self, name: &str) -> std::result::Result<Self::RepositoryStore, Self::Error>;
}

/// Provides access to a `Self::ManifestStore` and `Self::BlobStore` instances for the sake of
/// repository content management, handles session management (create, get, delete), and provides a
/// tag listing method.
///
#[async_trait]
pub trait RepositoryStore: Clone + Send + Sync + 'static {
    /// The type of the `ManifestStore` implementation provided by this `RepositoryStore`.
    type ManifestStore: ManifestStore;
    /// The type of the `BlobStore` implementation provided by this `RepositoryStore`.
    type BlobStore: BlobStore;
    /// The type of the `UploadSession` implementation provided by this `RepositoryStore`.
    type UploadSession: UploadSession + Send + Sync + 'static;

    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;

    /// The name of the repository accessed by this `RepositoryStore`
    fn name(&self) -> &str;

    /// Return a `Self::ManifestStore` to provide access to manifests in this repository.
    fn get_manifest_store(&self) -> Self::ManifestStore;

    /// Return a `Self::BlobStore` to provide access to blobs in this repository.
    fn get_blob_store(&self) -> Self::BlobStore;

    /// Return a list of tags in this repository.
    async fn get_tags(
        &self,
        n: Option<i64>,
        last: Option<String>,
    ) -> std::result::Result<TagList, Self::Error>;

    /// Initiate a new blob upload session.
    async fn new_upload_session(&self) -> std::result::Result<Self::UploadSession, Self::Error>;

    /// Get an existing blob upload session.
    async fn get_upload_session(
        &self,
        session_uuid: &Uuid,
    ) -> std::result::Result<Self::UploadSession, Self::Error>;

    /// Delete an existing blob upload session.
    async fn delete_session(&self, session_uuid: &Uuid) -> std::result::Result<(), Self::Error>;
}

#[async_trait]
pub trait ManifestStore: Send + Sync + 'static {
    type Manifest: Manifest;
    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;
    type ManifestBody: Stream<Item = std::result::Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>
        + Send;

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
    type Error: std::error::Error + Into<crate::errors::Error> + Send + Sync;
    type UploadSession: UploadSession + Send + Sync + 'static;
    type Blob: Blob;
    type BlobBody: Stream<Item = std::result::Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>
        + Send;

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

pub enum ManifestSpec {
    Image(ImageManifest),
    Index(ImageIndex),
}

impl TryFrom<&Bytes> for ManifestSpec {
    type Error = Error;

    fn try_from(bs: &Bytes) -> Result<Self> {
        let img_rej_err = match axum::Json::from_bytes(bs) {
            Ok(Json(m)) => return Ok(ManifestSpec::Image(m)),
            Err(e) => e,
        };
        match axum::Json::from_bytes(bs) {
            Ok(Json(m)) => return Ok(ManifestSpec::Index(m)),
            Err(ind_rej_err) => {
                tracing::warn!("unable to deserialize manifest as image: {img_rej_err:?}");
                tracing::warn!("unable to deserialize manifest as index: {ind_rej_err:?}");
                Err(Error::DistributionSpecError(
                    DistributionErrorCode::ManifestInvalid,
                ))
            }
        }
    }
}

impl ManifestSpec {
    #[inline(always)]
    pub fn media_type(&self) -> Option<MediaType> {
        match self {
            ManifestSpec::Image(im) => im.media_type().clone(),
            ManifestSpec::Index(ii) => ii.media_type().clone(),
        }
    }

    #[inline(always)]
    pub fn artifact_type(&self) -> Option<MediaType> {
        match self {
            ManifestSpec::Image(im) => im.artifact_type().clone(),
            ManifestSpec::Index(ii) => ii.artifact_type().clone(),
        }
    }

    #[inline(always)]
    pub fn annotations(&self) -> Option<HashMap<String, String>> {
        match self {
            ManifestSpec::Image(im) => im.annotations().clone(),
            ManifestSpec::Index(ii) => ii.annotations().clone(),
        }
    }

    #[inline(always)]
    pub fn subject(&self) -> Option<Descriptor> {
        match self {
            ManifestSpec::Image(im) => im.subject().clone(),
            ManifestSpec::Index(ii) => ii.subject().clone(),
        }
    }

    #[inline(always)]
    pub fn set_media_type(&mut self, s: &str) {
        let mt: MediaType = s.into();
        match self {
            ManifestSpec::Image(im) => {
                im.set_media_type(Some(mt));
            }
            ManifestSpec::Index(ii) => {
                ii.set_media_type(Some(mt));
            }
        }
    }

    pub fn infer_media_type(&mut self) -> Result<()> {
        tracing::info!("attempting to infer media type for manifest");
        match self {
            ManifestSpec::Image(im) => {
                // Content other than OCI container images MAY be packaged using the image
                // manifest. When this is done, the config.mediaType value MUST be set to a value
                // specific to the artifact type or the empty value. If the config.mediaType is set
                // to the empty value, the artifactType MUST be defined.
                if let Some(_artifact_type) = im.artifact_type() {
                    im.set_media_type(Some(MediaType::ImageManifest));
                } else if im.config().media_type() == &MediaType::EmptyJSON {
                    return Err(Error::DistributionSpecError(
                        DistributionErrorCode::ManifestInvalid,
                    ));
                }

                if im.config().media_type() == &MediaType::ImageConfig {
                    im.set_media_type(Some(MediaType::ImageManifest));
                    return Ok(());
                }

                Err(Error::DistributionSpecError(
                    DistributionErrorCode::ManifestInvalid,
                ))
            }
            ManifestSpec::Index(ii) => {
                ii.set_media_type(Some(MediaType::ImageIndex));
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub enum ManifestRef {
    Digest(OciDigest),
    Tag(String),
}

impl std::str::FromStr for ManifestRef {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if let Ok(dgst) = OciDigest::try_from(s) {
            return Ok(Self::Digest(dgst));
        }
        static RE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}").unwrap());

        if RE.is_match(s) {
            return Ok(Self::Tag(String::from(s)));
        }

        Err(Error::DistributionSpecError(
            DistributionErrorCode::ManifestInvalid,
        ))
    }
}
