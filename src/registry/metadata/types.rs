use once_cell::sync::Lazy;
use regex::Regex;
use sea_query::Iden;
use sqlx::Row;
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
    pub digest: OciDigest,
}

impl sqlx::FromRow<'_, sqlx_postgres::PgRow> for Blob {
    fn from_row(row: &sqlx_postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            registry_id: row.try_get("registry_id")?,
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

#[derive(Iden)]
pub enum Tags {
    Table,
    RepositoryId,
    ManifestId,
    Name,
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
    /// the id of the ObjectStore blob containing this manifest
    pub registry_id: Uuid,
    pub repository_id: Uuid,
    pub blob_id: Uuid,
    pub digest: OciDigest,
    pub media_type: Option<oci_spec::image::MediaType>,
    pub artifact_type: Option<oci_spec::image::MediaType>,
}

impl sqlx::FromRow<'_, sqlx_postgres::PgRow> for Manifest {
    fn from_row(row: &sqlx_postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            registry_id: row.try_get("registry_id")?,
            repository_id: row.try_get("repository_id")?,
            blob_id: row.try_get("blob_id")?,
            digest: match row.try_get::<String, &str>("digest")?.as_str().try_into() {
                Ok(v) => v,
                Err(e) => {
                    return Err(sqlx::Error::ColumnDecode {
                        index: "digest".to_string(),
                        source: Box::new(e),
                    })
                }
            },
            media_type: row
                .try_get::<Option<String>, &str>("media_type")?
                .map(|v| v.as_str().into()),
            artifact_type: row
                .try_get::<Option<String>, &str>("media_type")?
                .map(|v| v.as_str().into()),
        })
    }
}

#[derive(Iden)]
pub enum Manifests {
    Table,
    Id,
    RegistryId,
    BlobId,
    MediaType,
    ArtifactType,
    RepositoryId,
    Digest,
}
