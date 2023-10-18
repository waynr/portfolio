use std::collections::HashSet;

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use axum::body::Bytes;
use oci_spec::image::{Descriptor, ImageIndex, MediaType};

use super::blobs::PgS3BlobStore;
use super::errors::{Error, Result};
use super::metadata::Manifest;
use super::metadata::Repository;
use super::objects::ObjectStore;
use crate::oci_digest::OciDigest;
use crate::registry::{BlobStore, ManifestRef, ManifestSpec, ManifestStore};

pub struct PgS3ManifestStore {
    blobstore: PgS3BlobStore,
    repository: Repository,
}

impl PgS3ManifestStore {
    pub fn new(blobstore: PgS3BlobStore, repository: Repository) -> Self {
        Self {
            blobstore,
            repository,
        }
    }
}

#[async_trait]
impl ManifestStore for PgS3ManifestStore {
    type Manifest = Manifest;
    type Error = Error;

    async fn head(&self, key: &ManifestRef) -> Result<Option<Self::Manifest>> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        if let Some(manifest) = conn.get_manifest(&self.repository.id, key).await? {
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    async fn get(&self, key: &ManifestRef) -> Result<Option<(Self::Manifest, ByteStream)>> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        if let Some(manifest) = conn.get_manifest(&self.repository.id, key).await? {
            let body = self.blobstore.objects.get_blob(&manifest.blob_id).await?;
            Ok(Some((manifest, body)))
        } else {
            Ok(None)
        }
    }

    async fn put(
        &mut self,
        key: &ManifestRef,
        spec: &ManifestSpec,
        bytes: Bytes,
    ) -> Result<OciDigest> {
        let calculated_digest: OciDigest = bytes.as_ref().try_into()?;

        let byte_count = bytes.len();
        let blob_uuid = self
            .blobstore
            .put(&calculated_digest, byte_count as u64, bytes.into())
            .await?;

        let mut tx = self.blobstore.metadata.get_tx().await?;

        if let Some(m) = tx
            .get_manifest(
                &self.repository.id,
                &ManifestRef::Digest(calculated_digest.clone()),
            )
            .await?
        {
            return Ok(m.digest);
        }

        let manifest = Manifest::from_spec_with_params(
            spec,
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
                let blobs = tx.get_blobs(&digests).await?;

                let mut hs: HashSet<String> = HashSet::new();
                for blob in &blobs {
                    hs.insert((&blob.digest).into());
                }
                for digest in &digests {
                    if !hs.contains(*digest) {
                        tracing::warn!("blob for layer {digest} not found in repository");
                        return Err(Error::DistributionSpecError(
                            crate::DistributionErrorCode::BlobUnknown,
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
                let manifests = tx.get_manifests(&self.repository.id, &digests).await?;

                let mut hs: HashSet<String> = HashSet::new();
                for manifest in &manifests {
                    hs.insert((&manifest.digest).into());
                }
                for digest in &digests {
                    if !hs.contains(*digest) {
                        tracing::warn!("blob for manifest {digest} not found in repository");
                        return Err(Error::DistributionSpecError(
                            crate::DistributionErrorCode::ManifestUnknown,
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

    async fn delete(&mut self, key: &ManifestRef) -> Result<()> {
        let mut tx = self.blobstore.metadata.get_tx().await?;

        let manifest = tx.get_manifest(&self.repository.id, key).await?.ok_or(
            Error::DistributionSpecError(crate::DistributionErrorCode::ManifestUnknown),
        )?;

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

    async fn get_referrers(
        &self,
        subject: &OciDigest,
        artifact_type: Option<String>,
    ) -> Result<ImageIndex> {
        let mut index = ImageIndex::default();
        index.set_media_type(Some(MediaType::ImageIndex));

        let mut conn = self.blobstore.metadata.get_conn().await?;

        let manifests = conn
            .get_referrers(&self.repository.id, subject, &artifact_type)
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
