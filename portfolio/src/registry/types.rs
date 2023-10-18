use std::collections::HashMap;

use axum::body::Bytes;
use axum::Json;
use oci_spec::image::{Descriptor, ImageIndex, ImageManifest, MediaType};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;

use crate::errors::{DistributionErrorCode, Error, Result};
use crate::oci_digest::OciDigest;

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

#[derive(Serialize)]
pub struct TagsList {
    pub name: String,
    pub tags: Vec<String>,
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
