use axum::Json;
use axum::body::Bytes;

use crate::{
    errors::{DistributionErrorCode, Error, Result},
    metadata::{Manifest, ManifestRef, PostgresMetadata, Repository},
    objects::ObjectStore,
};

pub struct ManifestStore<'r, O>
where
    O: ObjectStore,
{
    metadata: PostgresMetadata,
    objects: O,
    repository: &'r Repository,
}

impl<'r, O> ManifestStore<'r, O>
where
    O: ObjectStore,
{
    pub fn new(metadata: PostgresMetadata, objects: O, repository: &'r Repository) -> Self {
        Self {
            metadata,
            objects,
            repository,
        }
    }

    pub async fn get_manifest(&self, key: &ManifestRef) -> Result<Option<Manifest>> {
        if let Some(mut manifest) = self
            .metadata
            .get_manifest(&self.repository.registry_id, &self.repository.id, key)
            .await?
        {
            let body = self.objects.get_blob(&manifest.config_blob_id).await?;
            manifest.body = Some(body);
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    pub async fn manifest_exists(&self, key: &ManifestRef) -> Result<Option<Manifest>> {
        if let Some(manifest) = self
            .metadata
            .get_manifest(&self.repository.registry_id, &self.repository.id, key)
            .await?
        {
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    pub async fn upload(&self, key: &ManifestRef, manifest: &ManifestSpec, bytes: Bytes) -> Result<()> {
        // TODO: eventually we'll need to check the mutability of a tag before overwriting it but
        // for now we overwrite it by default

        Ok(())
    }
}

pub enum ManifestSpec {
    Image(oci_spec::image::ImageManifest),
    Index(oci_spec::image::ImageIndex),
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
