use std::collections::HashMap;
use std::collections::HashSet;

use aws_sdk_s3::primitives::ByteStream;
use axum::body::Bytes;
use axum::Json;
use oci_spec::image::{Descriptor, ImageIndex, ImageManifest, MediaType};
use uuid::Uuid;

use crate::{
    errors::{DistributionErrorCode, Error, Result},
    metadata::{Manifest, ManifestRef, Repository},
    objects::ObjectStore,
    oci_digest::OciDigest,
    registry::BlobStore,
};

pub struct ManifestStore<'b, 'r, O>
where
    O: ObjectStore,
{
    blobstore: BlobStore<'b, O>,
    repository: &'r Repository,
}

impl<'b, 'r, O> ManifestStore<'b, 'r, O>
where
    O: ObjectStore,
{
    pub fn new(blobstore: BlobStore<'b, O>, repository: &'r Repository) -> Self {
        Self {
            blobstore,
            repository,
        }
    }

    pub async fn head_manifest(&self, key: &ManifestRef) -> Result<Option<Manifest>> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        if let Some(manifest) = conn
            .get_manifest(&self.repository.registry_id, &self.repository.id, key)
            .await?
        {
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    pub async fn get_manifest(&self, key: &ManifestRef) -> Result<Option<(Manifest, ByteStream)>> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        if let Some(manifest) = conn
            .get_manifest(&self.repository.registry_id, &self.repository.id, key)
            .await?
        {
            let body = self.blobstore.objects.get_blob(&manifest.blob_id).await?;
            Ok(Some((manifest, body)))
        } else {
            Ok(None)
        }
    }

    pub async fn manifest_exists(&self, key: &ManifestRef) -> Result<Option<Manifest>> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        if let Some(manifest) = conn
            .get_manifest(&self.repository.registry_id, &self.repository.id, key)
            .await?
        {
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    pub async fn upload(
        &mut self,
        key: &ManifestRef,
        spec: &ManifestSpec,
        bytes: Bytes,
    ) -> Result<OciDigest> {
        let calculated_digest: OciDigest = bytes.as_ref().try_into()?;

        let byte_count = bytes.len();
        let blob_uuid = self
            .blobstore
            .upload(&calculated_digest, byte_count as u64, bytes.into())
            .await?;

        let mut tx = self.blobstore.metadata.get_tx().await?;

        if let Some(m) = tx
            .get_manifest(
                &self.repository.registry_id,
                &self.repository.id,
                &ManifestRef::Digest(calculated_digest.clone()),
            )
            .await?
        {
            return Ok(m.digest);
        }

        let manifest: Manifest = spec.new_manifest(
            self.repository.registry_id,
            self.repository.id,
            blob_uuid,
            calculated_digest.clone(),
            byte_count as i64,
        );
        tx.insert_manifest(&manifest).await?;

        match spec {
            ManifestSpec::Image(img) => {
                let layers = img.layers();

                // first ensure all referenced layers exist as blobs
                let digests = layers.iter().map(|desc| desc.digest().as_str()).collect();
                let blobs = tx.get_blobs(&self.repository.registry_id, &digests).await?;

                let mut hs = HashSet::new();
                for blob in &blobs {
                    hs.insert(blob.digest.as_str());
                }
                for digest in &digests {
                    if !hs.contains(*digest) {
                        return Err(Error::DistributionSpecError(
                            DistributionErrorCode::BlobUnknown,
                        ));
                    }
                }

                // then associate all blobs with the manifest in the database
                let blob_uuids = blobs.iter().map(|b| &b.id).collect();

                tx.associate_image_layers(&manifest.id, blob_uuids).await?;
            }
            ManifestSpec::Index(ind) => {
                let manifests = ind.manifests();

                // first ensure all referenced manifests exist as blobs
                let digests = manifests
                    .iter()
                    .map(|desc| desc.digest().as_str())
                    .collect();
                let manifests = tx
                    .get_manifests(&self.repository.registry_id, &self.repository.id, &digests)
                    .await?;

                let mut hs: HashSet<String> = HashSet::new();
                for manifest in &manifests {
                    hs.insert((&manifest.digest).into());
                }
                for digest in &digests {
                    if !hs.contains(*digest) {
                        return Err(Error::DistributionSpecError(
                            DistributionErrorCode::ManifestUnknown,
                        ));
                    }
                }

                // then associate all blobs with the manifest in the database
                let manifest_uuids = manifests.iter().map(|b| &b.id).collect();

                tx.associate_index_manifests(&manifest.id, manifest_uuids)
                    .await?;
            }
        }

        if let ManifestRef::Tag(t) = key {
            // TODO: eventually we'll need to check the mutability of a tag before overwriting it
            // but for now we overwrite it by default
            tx.upsert_tag(&self.repository.id, &manifest.id, t.as_str())
                .await?;
        }

        tx.commit().await?;

        Ok(calculated_digest)
    }

    pub async fn delete(&mut self, key: &ManifestRef) -> Result<()> {
        let mut tx = self.blobstore.metadata.get_tx().await?;

        let manifest = tx
            .get_manifest(&self.repository.registry_id, &self.repository.id, key)
            .await?
            .ok_or(Error::DistributionSpecError(
                DistributionErrorCode::ManifestUnknown,
            ))?;

        // NOTE: it's possible (but how likely?) for a manifest to include both layers and
        // manifests; we don't support creating both types of association for now, but we should
        // support deleting them here just in case
        tx.delete_image_layers(&manifest.id).await?;
        tx.delete_index_manifests(&manifest.id).await?;
        tx.delete_tags_by_manifest_id(&manifest.id).await?;
        tx.delete_manifest(&manifest.id).await?;
        tx.delete_blob(&manifest.blob_id).await?;

        let mut count = 0;
        while self
            .blobstore
            .objects
            .blob_exists(&manifest.blob_id)
            .await?
            && count < 10
        {
            self.blobstore
                .objects
                .delete_blob(&manifest.blob_id)
                .await?;
            count += 1;
        }

        tx.commit().await?;

        Ok(())
    }

    pub(crate) async fn get_referrers(
        &self,
        subject: &OciDigest,
        artifact_type: Option<String>,
    ) -> Result<ImageIndex> {
        let mut index = ImageIndex::default();
        index.set_media_type(Some(MediaType::ImageIndex));

        let mut conn = self.blobstore.metadata.get_conn().await?;

        let manifests = conn
            .get_referrers(
                &self.repository.registry_id,
                &self.repository.id,
                subject,
                &artifact_type,
            )
            .await?;
        let count = manifests.len();

        let set = &mut tokio::task::JoinSet::new();
        for m in manifests.into_iter() {
            let objects = self.blobstore.objects.clone();
            if m.media_type.is_none() {
                tracing::warn!(
                    "manifest {} (digest {:?}) unexpectedly missing media type!",
                    m.id,
                    m.digest
                );
                continue;
            }
            let db_media_type = m.media_type.unwrap();
            set.spawn(async move {
                let stream = objects.get_blob(&m.blob_id).await?;
                let bs = stream.collect().await.map(|d| d.into_bytes())?;
                let spec = ManifestSpec::try_from(&bs)?;
                let media_type = spec.media_type().unwrap_or(db_media_type);
                let mut d = Descriptor::new(media_type, bs.len() as i64, &m.digest);
                d.set_artifact_type(spec.artifact_type());
                d.set_annotations(spec.annotations());
                Ok(d)
            });
        }

        let mut ds: Vec<Descriptor> = Vec::with_capacity(count);
        while let Some(res) = set.join_next().await {
            let d = match res {
                Err(e @ tokio::task::JoinError { .. }) => {
                    if e.is_panic() {
                        tracing::error!(
                            "manifest deserialization task panicked while getting referrers for {:?}",
                            subject
                        );
                    }
                    return Err(e.into());
                }
                Ok(Err(e)) => return Err(e),
                Ok(Ok(d)) => d,
            };
            ds.push(d);
        }

        ds.sort_unstable_by(|left, right| left.digest().cmp(right.digest()));
        index.set_manifests(ds);

        Ok(index)
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
        registry_id: Uuid,
        repository_id: Uuid,
        blob_id: Uuid,
        dgst: OciDigest,
        bytes_on_disk: i64,
    ) -> Manifest {
        match self {
            ManifestSpec::Image(img) => Manifest {
                id: Uuid::new_v4(),
                registry_id,
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
                registry_id,
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
