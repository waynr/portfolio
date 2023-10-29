use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use oci_spec::distribution::{TagList, TagListBuilder};
use oci_spec::image::{Descriptor, ImageIndex, MediaType};

use portfolio_core::registry::{BlobStore, ManifestRef, ManifestSpec, ManifestStore};
use portfolio_core::Error as CoreError;
use portfolio_core::OciDigest;
use portfolio_objectstore::Key;

use super::blobs::PgBlobStore;
use super::errors::{Error, Result};
use super::metadata::Manifest;
use super::metadata::Repository;

pub struct PgManifestStore {
    blobstore: PgBlobStore,
    repository: Repository,
}

impl PgManifestStore {
    pub fn new(blobstore: PgBlobStore, repository: Repository) -> Self {
        Self {
            blobstore,
            repository,
        }
    }
}

type TryBytes = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
impl ManifestStore for PgManifestStore {
    type Manifest = Manifest;
    type Error = Error;
    type ManifestBody = BoxStream<'static, TryBytes>;

    async fn head(&self, key: &ManifestRef) -> Result<Option<Self::Manifest>> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        if let Some(manifest) = conn.get_manifest(&self.repository.id, key).await? {
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    async fn get(&self, key: &ManifestRef) -> Result<Option<(Self::Manifest, Self::ManifestBody)>> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        if let Some(manifest) = conn.get_manifest(&self.repository.id, key).await? {
            let body = self
                .blobstore
                .objects
                .get(&Key::from(&manifest.blob_id))
                .await?;
            Ok(Some((manifest, body.map_err(|e| e.into()).boxed())))
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
                        let msg = format!("blob for layer {digest} not found in repository");
                        tracing::warn!("{msg}");
                        return Err(CoreError::ManifestBlobUnknown(Some(msg)).into());
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
                        let msg = format!("blob for manifest {digest} not found in repository");
                        tracing::warn!("{msg}");
                        return Err(CoreError::ManifestUnknown(Some(msg)).into());
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

        let manifest = tx
            .get_manifest(&self.repository.id, key)
            .await?
            .ok_or(CoreError::ManifestUnknown(None))?;

        // NOTE: it's possible (but how likely?) for a manifest to include both layers and
        // manifests; we don't support creating both types of association for now, but we should
        // support deleting them here just in case
        tx.delete_image_layers(&manifest.id).await?;
        tx.delete_index_manifests(&manifest.id).await?;
        tx.delete_tags_by_manifest_id(&manifest.id).await?;
        tx.delete_manifest(&manifest.id).await?;
        tx.delete_blob(&manifest.blob_id).await?;

        let manifest_blob_key = Key::from(&manifest.blob_id);

        let mut count = 0;
        while self.blobstore.objects.exists(&manifest_blob_key).await? && count < 10 {
            self.blobstore.objects.delete(&manifest_blob_key).await?;
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
                let stream = objects.get(&Key::from(&m.blob_id)).await?;
                let bs: Bytes = stream
                    .try_collect::<Vec<Bytes>>()
                    .await?
                    .into_iter()
                    .fold(BytesMut::new(), |mut acc, bs| {
                        acc.extend_from_slice(&bs);
                        acc
                    })
                    .into();
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

    async fn get_tags(&self, n: Option<i64>, last: Option<String>) -> Result<TagList> {
        let mut conn = self.blobstore.metadata.get_conn().await?;
        let taglist = TagListBuilder::default()
            .name(self.repository.name.as_str())
            .tags(
                conn.get_tags(&self.repository.id, n, last)
                    .await?
                    .into_iter()
                    .map(|t| t.name)
                    .collect::<Vec<_>>(),
            )
            .build()?;

        Ok(taglist)
    }
}
