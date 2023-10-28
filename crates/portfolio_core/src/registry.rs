//! # Registry Abstractions
//!
//! Defines the basic interoperability layer between [`portfolio_http`] and backend
//! implementations.
//!
//! Eventually these types are intended to be useful as an interoperability layer between backend
//! implementations as well -- for example, making it possible to safely transfer distribution data
//! between backends themselves.
//!
//! ## Known Implementations
//!
//! ### portfolio_backend_postgres
//!
//! Implementation of the traits defined here that distinguishes metadata from bulk data storage.
//!
//! Metadata is stored in a Postgres database in a way that supports a limited form of ACID
//! transactional safety on Distribution API operations (eg conflicts between write and delete
//! operations are detected and resolved via Postgres transactions).
//!
//! Metadata consists of:
//!
//! * relationships between images, manifests, blobs, layers, indices, index manifests, tags, and
//! repositories
//! * chunked upload session data, including upload id, most recent chunk number, last byte
//! uploaded, and digest state
//!
//! Bulk data storage is handled via the [`postgres_objectstore`] crate, which itself abstracts
//! over different kinds of bulk data store via its
//! [`ObjectStore`](postgres_objectstore::ObjectStore) trait.
use std::collections::HashMap;

use async_trait::async_trait;
use axum::Json;
use bytes::Bytes;
use futures_core::Stream;
use hyper::body::Body;
use oci_spec::distribution::TagList;
use oci_spec::image::{Descriptor, ImageIndex, ImageManifest, MediaType};
use once_cell::sync::Lazy;
use regex::Regex;
use uuid::Uuid;

use crate::errors::{DistributionErrorCode, Error, Result};
use crate::oci_digest::OciDigest;

/// Provide access to [`RepositoryStore`] instances.
///
/// Backend implementations may impose their own access control and repository limit policies.
/// Intended to be used within [`portfolio_http`]'s HTTP middleware to inject [`RepositoryStore`]
/// instances into the request handling chain for use by Distribution API route handlers.
#[async_trait]
pub trait RepositoryStoreManager: Clone + Send + Sync + 'static {
    /// The `RepositoryStore` implementation an implementing type provides.
    type RepositoryStore: RepositoryStore;

    type Error: std::error::Error + Into<crate::errors::RepositoryError> + Send + Sync;

    /// Get [`RepositoryStore`] corresponding to the given name, if it already exists. This name
    /// corresponds to the `<name>` in distribution-spec API endpoints like
    /// `/v2/<name>/blobs/<digest>`.
    async fn get(
        &self,
        name: &str,
    ) -> std::result::Result<Option<Self::RepositoryStore>, Self::Error>;

    /// Create new [`RepositoryStore`] with the given name. This name corresponds to the
    /// `<name>` in distribution-spec API endpoints like `/v2/<name>/blobs/<digest>`.
    async fn create(&self, name: &str) -> std::result::Result<Self::RepositoryStore, Self::Error>;
}

/// Provides access to a [`ManifestStore`] and [`BlobStore`] instances for a repository.
///
/// Enables management of content within a registry scoped to a specific repository. It also
/// handles session management (create, get, delete), and provides a tag listing method.
#[async_trait]
pub trait RepositoryStore: Clone + Send + Sync + 'static {
    /// The type of the [`ManifestStore`] implementation provided by this [`RepositoryStore`].
    type ManifestStore: ManifestStore;
    /// The type of the [`BlobStore`] implementation provided by this [`RepositoryStore`].
    type BlobStore: BlobStore;
    /// The type of the [`BlobStore`] implementation provided by this [`RepositoryStore`].
    type UploadSessionStore: UploadSessionStore;

    type Error: std::error::Error + Into<crate::errors::RepositoryError> + Send + Sync;

    /// The name of the repository accessed by this [`RepositoryStore`].
    fn name(&self) -> &str;

    /// Return a [`Self::ManifestStore`] to provide access to manifests in this repository.
    fn get_manifest_store(&self) -> Self::ManifestStore;

    /// Return a [`Self::BlobStore`] to provide access to blobs in this repository.
    fn get_blob_store(&self) -> Self::BlobStore;

    /// Return a [`Self::UploadSessionStore`] to provide access to blobs in this repository.
    fn get_upload_session_store(&self) -> Self::UploadSessionStore;
}

/// Provides access to upload sessions.
#[async_trait]
pub trait UploadSessionStore: Clone + Send + Sync + 'static {
    /// The type of the [`UploadSession`] implementation provided by this [`RepositoryStore`].
    type UploadSession: UploadSession + Send + Sync + 'static;

    type Error: std::error::Error + Into<crate::errors::BlobError> + Send + Sync;

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

/// Provides access to registry manifests.
#[async_trait]
pub trait ManifestStore: Send + Sync + 'static {
    type Manifest: Manifest;
    type Error: std::error::Error + Into<crate::errors::ManifestError> + Send + Sync;
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

    /// Return an ImageIndex containing a list of manifests that reference the given OciDigest.
    async fn get_referrers(
        &self,
        subject: &OciDigest,
        artifact_type: Option<String>,
    ) -> std::result::Result<ImageIndex, Self::Error>;

    /// Return an OCI TagList of tags in this repository.
    async fn get_tags(
        &self,
        n: Option<i64>,
        last: Option<String>,
    ) -> std::result::Result<TagList, Self::Error>;
}

/// Provides access to registry blobs.
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    type BlobWriter: BlobWriter;
    type Error: std::error::Error + Into<crate::errors::BlobError> + Send + Sync;
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

/// Implements chunked blob uploads.
#[async_trait]
pub trait BlobWriter: Send + Sync + 'static {
    type Error: std::error::Error + Into<crate::errors::BlobError> + Send + Sync;
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

/// Provides access to blob metadata.
pub trait Blob {
    fn bytes_on_disk(&self) -> u64;
}

/// Provides access to manifest metadata.
pub trait Manifest {
    fn bytes_on_disk(&self) -> u64;
    fn digest(&self) -> &OciDigest;
    fn media_type(&self) -> &Option<MediaType>;
}

/// Provides access to blob upload session metadata.
pub trait UploadSession {
    fn uuid(&self) -> &Uuid;
    fn upload_id(&self) -> &Option<String>;
    fn last_range_end(&self) -> i64;
}

/// Abstraction over [`oci_spec::image::ImageManifest`] and [`oci_spec::image::ImageIndex`].
///
/// Provides methods to access metadata relevant for implementing Distribution HTTP API and
/// Portfolio backends.
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

    /// Attempt to infer the media type of the Manifest if not present. Based on the rules outlined
    /// in the [OCI Image Manifest
    /// specification](https://github.com/opencontainers/image-spec/blob/main/manifest.md).
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

/// Reference to an [OCI
/// Manifest](https://github.com/opencontainers/image-spec/blob/main/manifest.md) as specified by
/// the [OCI Distrbution Spec](https://github.com/opencontainers/distribution-spec).
///
/// This refers to the `<reference>` portion of distribution API endpoints taking the form
/// `/v2/<name>/manifests/<reference>`. According to the distribution specification:
///
/// > `<reference>` MUST be either (a) the digest of the manifest or (b) a tag. The `<reference>` MUST
/// > NOT be in any other format.
///
/// and
///
/// > Throughout this document, `<reference>` as a tag MUST be at most 128 characters in length and
/// > MUST match the following regular expression:
/// >
/// > `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`
#[derive(Debug)]
pub enum ManifestRef {
    Digest(OciDigest),
    Tag(String),
}

impl std::str::FromStr for ManifestRef {
    type Err = Error;

    /// Convert [`&str`] to a [`ManifestRef`] first by attempting to convert into
    /// [`super::OciDigest`] then if that doesn't work, checking that the string is a valid
    /// distribution tag using the regex `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`.
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
