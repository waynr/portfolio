use aws_sdk_s3::primitives::ByteStream;
use axum::body::StreamBody;
use once_cell::sync::Lazy;
use regex::Regex;
use uuid::Uuid;

use crate::errors::{DistributionErrorCode, Error};
use crate::oci_digest::OciDigest;

#[derive(Clone)]
pub struct Registry {
    pub(crate) id: Uuid,
    pub name: String,
}

#[derive(Clone)]
pub struct Repository {
    pub(crate) id: Uuid,
    pub registry_id: Uuid,
    pub name: String,
}

pub struct Blob {
    pub id: Uuid,
    pub registry_id: Uuid,
    pub uploaded: bool,
    pub digest: String,
}

pub enum ManifestRef {
    Digest(OciDigest),
    Tag(String),
}

impl std::str::FromStr for ManifestRef {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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

pub struct ImageManifest {
    pub id: Uuid,
    pub registry_id: Uuid,
    pub repository_id: Uuid,
    pub config_blob_id: Uuid,
    pub digest: OciDigest,
    pub media_type: oci_spec::image::MediaType,
    pub artifact_type: Option<oci_spec::image::MediaType>,
    pub body: Option<StreamBody<ByteStream>>,
}
