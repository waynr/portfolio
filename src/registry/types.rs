use axum::body::Bytes;
use axum::Json;
use oci_spec::image::{Descriptor, ImageIndex, ImageManifest, MediaType};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;
use uuid::Uuid;

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
    pub(crate) fn media_type(&self) -> Option<MediaType> {
        match self {
            ManifestSpec::Image(im) => im.media_type().clone(),
            ManifestSpec::Index(ii) => ii.media_type().clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn artifact_type(&self) -> Option<MediaType> {
        match self {
            ManifestSpec::Image(im) => im.artifact_type().clone(),
            ManifestSpec::Index(ii) => ii.artifact_type().clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn annotations(&self) -> Option<HashMap<String, String>> {
        match self {
            ManifestSpec::Image(im) => im.annotations().clone(),
            ManifestSpec::Index(ii) => ii.annotations().clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn subject(&self) -> Option<Descriptor> {
        match self {
            ManifestSpec::Image(im) => im.subject().clone(),
            ManifestSpec::Index(ii) => ii.subject().clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn set_media_type(&mut self, s: &str) {
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

    pub(crate) fn infer_media_type(&mut self) -> Result<()> {
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

    pub(crate) fn new_manifest(
        &self,
        repository_id: Uuid,
        blob_id: Uuid,
        dgst: OciDigest,
        bytes_on_disk: i64,
    ) -> Manifest {
        match self {
            ManifestSpec::Image(img) => Manifest {
                id: Uuid::new_v4(),
                repository_id,
                blob_id,
                bytes_on_disk,
                digest: dgst,
                subject: img.subject().as_ref().map(|v| {
                    v.digest()
                        .as_str()
                        .try_into()
                        .expect("valid descriptor digest will always product valid OciDigest")
                }),
                media_type: img.media_type().clone(),
                artifact_type: img.artifact_type().clone(),
            },
            ManifestSpec::Index(ind) => Manifest {
                id: Uuid::new_v4(),
                repository_id,
                blob_id,
                bytes_on_disk,
                digest: dgst,
                subject: ind.subject().as_ref().map(|v| {
                    v.digest()
                        .as_str()
                        .try_into()
                        .expect("valid descriptor digest will always product valid OciDigest")
                }),
                media_type: ind.media_type().clone(),
                artifact_type: ind.artifact_type().clone(),
            },
        }
    }
}

#[derive(Serialize)]
pub struct TagsList {
    pub name: String,
    pub tags: Vec<String>,
}

#[derive(sqlx::FromRow, Clone)]
pub struct Repository {
    pub(crate) id: Uuid,
    pub name: String,
}

pub struct Blob {
    pub id: Uuid,
    pub digest: OciDigest,
    pub bytes_on_disk: i64,
}

impl sqlx::FromRow<'_, sqlx_postgres::PgRow> for Blob {
    fn from_row(row: &sqlx_postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            digest: match row.try_get::<String, &str>("digest")?.as_str().try_into() {
                Ok(v) => v,
                Err(e) => {
                    return Err(sqlx::Error::ColumnDecode {
                        index: "digest".to_string(),
                        source: Box::new(e),
                    })
                }
            },
            bytes_on_disk: row.try_get("bytes_on_disk")?,
        })
    }
}

#[derive(Debug)]
pub struct Tag {
    pub manifest_id: Uuid,
    pub name: String,
    pub digest: OciDigest,
}

impl sqlx::FromRow<'_, sqlx_postgres::PgRow> for Tag {
    fn from_row(row: &sqlx_postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            manifest_id: row.try_get("manifest_id")?,
            name: row.try_get("name")?,
            digest: match row.try_get::<String, &str>("digest")?.as_str().try_into() {
                Ok(v) => v,
                Err(e) => {
                    return Err(sqlx::Error::ColumnDecode {
                        index: "digest".to_string(),
                        source: Box::new(e),
                    })
                }
            },
        })
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

pub struct Manifest {
    pub id: Uuid,
    pub repository_id: Uuid,
    /// the id of the ObjectStore blob containing this manifest
    pub blob_id: Uuid,
    pub bytes_on_disk: i64,
    pub digest: OciDigest,
    pub subject: Option<OciDigest>,
    pub media_type: Option<oci_spec::image::MediaType>,
    pub artifact_type: Option<oci_spec::image::MediaType>,
}

impl sqlx::FromRow<'_, sqlx_postgres::PgRow> for Manifest {
    fn from_row(row: &sqlx_postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            repository_id: row.try_get("repository_id")?,
            blob_id: row.try_get("blob_id")?,
            bytes_on_disk: row.try_get("bytes_on_disk")?,
            digest: match row.try_get::<String, _>("digest")?.as_str().try_into() {
                Ok(v) => v,
                Err(e) => {
                    return Err(sqlx::Error::ColumnDecode {
                        index: "digest".to_string(),
                        source: Box::new(e),
                    })
                }
            },
            subject: row
                .try_get::<Option<String>, _>("subject")?
                .map(|v| match OciDigest::try_from(v.as_str()) {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        return Err(sqlx::Error::ColumnDecode {
                            index: "subject".to_string(),
                            source: Box::new(e),
                        })
                    }
                })
                .transpose()?,
            media_type: row
                .try_get::<Option<String>, _>("media_type")?
                .map(|v| v.as_str().into()),
            artifact_type: row
                .try_get::<Option<String>, _>("media_type")?
                .map(|v| v.as_str().into()),
        })
    }
}
