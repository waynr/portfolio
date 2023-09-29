use axum::body::Bytes;
use axum::Json;
use oci_spec::image::{ImageIndex, ImageManifest, MediaType};

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

    pub async fn upload(
        &self,
        key: &ManifestRef,
        manifest: &ManifestSpec,
        bytes: Bytes,
    ) -> Result<()> {
        // TODO: eventually we'll need to check the mutability of a tag before overwriting it but
        // for now we overwrite it by default

        Ok(())
    }
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
    pub(crate) fn media_type(&self) -> Option<MediaType> {
        match self {
            ManifestSpec::Image(im) => im.media_type().clone(),
            ManifestSpec::Index(ii) => ii.media_type().clone(),
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
}
