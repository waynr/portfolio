use oci_spec::image::MediaType;
use sea_query::Iden;
use sqlx::Row;
use uuid::Uuid;

use crate::oci_digest::OciDigest;
use crate::registry;
use crate::registry::ManifestSpec;

#[derive(sqlx::FromRow, Clone)]
pub struct Repository {
    pub(crate) id: Uuid,
    pub name: String,
}

#[derive(Iden)]
pub enum Repositories {
    Table,
    Id,
    Name,
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

impl registry::Blob for Blob {
    #[inline]
    fn bytes_on_disk(&self) -> u64 {
        self.bytes_on_disk as u64
    }
}

#[derive(Iden)]
pub enum Blobs {
    Table,
    Id,
    Digest,
    BytesOnDisk,
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

impl registry::Manifest for Manifest {
    #[inline]
    fn bytes_on_disk(&self) -> u64 {
        self.bytes_on_disk as u64
    }

    #[inline]
    fn digest(&self) -> &OciDigest {
        &self.digest
    }

    #[inline]
    fn media_type(&self) -> &Option<MediaType> {
        &self.media_type
    }
}

impl Manifest {
    pub(crate) fn from_spec_with_params(
        spec: &ManifestSpec,
        repository_id: Uuid,
        blob_id: Uuid,
        dgst: OciDigest,
        bytes_on_disk: i64,
    ) -> Self {
        match spec {
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

#[derive(Iden)]
pub enum Manifests {
    Table,
    Id,
    BlobId,
    MediaType,
    ArtifactType,
    RepositoryId,
    Digest,
    Subject,
}

#[derive(Iden)]
pub enum Layers {
    Table,
    Manifest,
    Blob,
}

#[derive(Iden)]
pub enum IndexManifests {
    Table,
    ParentManifest,
    ChildManifest,
}
