use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use bytes::BytesMut;
use futures::stream::TryStreamExt;
use hyper::body::Body;
use oci_spec::image::History;
use oci_spec::image::ImageConfiguration;
use oci_spec::image::ImageIndex;
use oci_spec::image::ImageManifest;
use oci_spec::image::MediaType;
use oci_spec::image::{Descriptor, DescriptorBuilder};
use tokio::task::JoinSet;

use portfolio_core::registry::BlobStore;
use portfolio_core::registry::BoxedRepositoryStoreManager;
use portfolio_core::registry::ManifestRef;
use portfolio_core::registry::ManifestSpec;
use portfolio_core::registry::ManifestStore;
use portfolio_core::registry::{BoxedRepositoryStore, RepositoryStoreManager};
use portfolio_core::OciDigest;

pub use super::errors::{Error, Result};
use super::{Image, Index, Layer};

pub(crate) type ArcRepositoryStoreManager = Arc<dyn RepositoryStoreManager + Send + Sync>;
pub(crate) type ArcManifestStore = Arc<dyn ManifestStore + Send + Sync>;
pub(crate) type ArcBlobStore = Arc<dyn BlobStore + Send + Sync>;

#[derive(Clone)]
pub struct RepositoryLoader {
    mgr: ArcRepositoryStoreManager,
}

impl RepositoryLoader {
    pub fn new(mgr: BoxedRepositoryStoreManager) -> Self {
        Self {
            mgr: Arc::from(mgr),
        }
    }

    pub async fn get_manifest_store(&self, repo_name: &str) -> ArcManifestStore {
        let repo_store = self
            .get_or_create_repo(repo_name)
            .await
            .expect("must be able to get or create repo");
        Arc::from(repo_store.get_manifest_store())
    }

    pub async fn get_blob_store(&self, repo_name: &str) -> ArcBlobStore {
        let repo_store = self
            .get_or_create_repo(repo_name)
            .await
            .expect("must be able to get or create repo");
        Arc::from(repo_store.get_blob_store())
    }

    pub async fn get_or_create_repo(&self, name: &str) -> Result<BoxedRepositoryStore> {
        if let Some(repo) = self.mgr.get(name).await? {
            Ok(repo)
        } else {
            Ok(self.mgr.create("repo_1").await?)
        }
    }

    async fn upload_layer(blob_store: ArcBlobStore, layer: Arc<Mutex<Layer>>) -> Result<()> {
        let (descriptor, data) = {
            let mut layer = layer.lock().unwrap();
            (layer.descriptor(), layer.data.clone())
        };
        tracing::info!(
            "pushing image layer: {}\n with contents\n {}",
            descriptor.digest(),
            data,
        );
        blob_store
            .put(
                &descriptor.digest().as_str().try_into()?,
                data.len() as u64,
                Body::from(data.clone()),
            )
            .await?;
        Ok(())
    }

    async fn upload_image(
        manifest_store: ArcManifestStore,
        blob_store: ArcBlobStore,
        image: Arc<Mutex<Image>>,
    ) -> Result<()> {
        tracing::info!("pushing image: {:?}", image.lock().unwrap().manifest_ref());

        let mut set = JoinSet::new();
        for layer in &image.lock().unwrap().layers {
            let blob_store = blob_store.clone();
            let layer = layer.clone();
            set.spawn(async move { Self::upload_layer(blob_store, layer).await });
        }

        let manifest = image.lock().unwrap().manifest();
        let digest = manifest.config().digest();
        tracing::info!("pushing image config: {}", digest,);

        let config = image.lock().unwrap().config();
        let config_bytes = serde_json::to_vec(&config)?;
        let oci_digest: OciDigest = digest.as_str().try_into()?;

        set.spawn(
            async move { Self::upload_image_config(blob_store, &oci_digest, config_bytes).await },
        );

        let manifest_bytes = serde_json::to_vec(&manifest)?;

        while let Some(res) = set.join_next().await {
            match res {
                Err(e) => return Err(e.into()),
                _ => (),
            }
        }

        tracing::info!(
            "pushing image manifest: {:?}",
            image.lock().unwrap().manifest_ref()
        );
        let manifest_ref = { &image.lock().unwrap().manifest_ref().clone() };
        manifest_store
            .put(
                &manifest_ref,
                &ManifestSpec::Image(manifest),
                Bytes::from(manifest_bytes),
            )
            .await?;
        Ok(())
    }

    async fn upload_image_config(
        blob_store: ArcBlobStore,
        oci_digest: &OciDigest,
        config_bytes: Vec<u8>,
    ) -> Result<()> {
        blob_store
            .put(
                oci_digest,
                config_bytes.len() as u64,
                Body::from(config_bytes),
            )
            .await?;
        Ok(())
    }

    pub async fn upload_images(
        self,
        repo_name: String,
        images: Vec<Arc<Mutex<Image>>>,
    ) -> Result<()> {
        let manifest_store = self.get_manifest_store(&repo_name).await;
        let blob_store = self.get_blob_store(&repo_name).await;

        let mut set = JoinSet::new();
        for image in images {
            let _ = image.lock().unwrap_or_else(|e| e.into_inner()).descriptor();
            let manifest_store = manifest_store.clone();
            let blob_store = blob_store.clone();
            let image = image.clone();
            set.spawn(async move { Self::upload_image(manifest_store, blob_store, image).await });
        }
        while let Some(res) = set.join_next().await {
            match res {
                Err(e) => return Err(e.into()),
                _ => (),
            }
        }
        Ok(())
    }

    async fn upload_index(self, repo_name: String, index: Arc<Mutex<Index>>) -> Result<()> {
        let loader = self.clone();
        let mut set = JoinSet::new();
        {
            let repo_name = repo_name.clone();
            let manifests = index.lock().unwrap().manifests.clone();
            set.spawn(loader.upload_images(repo_name, manifests));
        }
        let (manifest, manifest_ref) = {
            let mut index = index.lock().unwrap();
            (index.manifest(), index.manifest_ref())
        };
        let manifest_bytes = serde_json::to_vec(&manifest)?;

        while let Some(res) = set.join_next().await {
            match res {
                Err(e) => return Err(e.into()),
                _ => (),
            }
        }

        let manifest_store = self.get_manifest_store(&repo_name).await;
        tracing::info!("pushing index manifest: {:?}", manifest_ref);
        manifest_store
            .put(
                &manifest_ref,
                &ManifestSpec::Index(manifest),
                Bytes::from(manifest_bytes),
            )
            .await?;
        Ok(())
    }

    pub async fn upload_indices(
        &self,
        repo_name: String,
        indices: Vec<Arc<Mutex<Index>>>,
    ) -> Result<()> {
        let mut set = JoinSet::new();
        for index in indices {
            let repo_name = repo_name.clone();
            let index = index.clone();
            let loader = self.clone();
            set.spawn(loader.upload_index(repo_name, index));
        }
        while let Some(res) = set.join_next().await {
            match res {
                Err(e) => return Err(e.into()),
                _ => (),
            }
        }
        Ok(())
    }

    pub async fn pull_image(&self, name: String, manifest_ref: &ManifestRef) -> Result<Image> {
        let manifest_store = self.get_manifest_store(&name).await;
        let blob_store = self.get_blob_store(&name).await;

        // pull manifest
        let manifest_stream = match manifest_store.get(manifest_ref).await? {
            Some((_, stream)) => stream,
            None => return Err(Error::ManifestNotFound(format!("{:?}", manifest_ref))),
        };
        let manifest_bytes: BytesMut = manifest_stream
            .try_collect()
            .await
            .map_err(|e| Error::StreamCollectFailed(format!("{e:?}")))?;
        let manifest: ImageManifest = serde_json::from_slice(&manifest_bytes)?;
        let manifest_digest = OciDigest::from(manifest_bytes.as_ref());

        let descriptor = DescriptorBuilder::default()
            .media_type(MediaType::ImageManifest)
            .digest(String::from(&manifest_digest).as_str())
            .size(manifest_bytes.len() as i64)
            .build()
            .expect("must set all required fields for descriptor");

        let config_digest: OciDigest = manifest.config().digest().as_str().try_into()?;
        let config_blob_stream = match blob_store.get(&config_digest).await? {
            Some((_, stream)) => stream,
            None => return Err(Error::BlobNotFound(format!("{:?}", config_digest))),
        };
        let config_blob_bytes: BytesMut = config_blob_stream
            .try_collect()
            .await
            .map_err(|e| Error::StreamCollectFailed(format!("{e:?}")))?;
        let image_config: ImageConfiguration = serde_json::from_slice(&config_blob_bytes)?;

        let layer_descriptors: Vec<(Descriptor, Option<History>)> =
            if image_config.history().len() == manifest.layers().len() {
                manifest
                    .layers()
                    .iter()
                    .map(Clone::clone)
                    .zip(image_config.history().iter().map(Clone::clone).map(Some))
                    .collect()
            } else {
                manifest
                    .layers()
                    .iter()
                    .map(Clone::clone)
                    .zip(std::iter::repeat(None))
                    .collect()
            };

        // for each layer in manifest, pull layer
        let mut layers: Vec<Arc<Mutex<Layer>>> = Vec::new();

        for (layer, history) in layer_descriptors {
            let digest: OciDigest = layer.digest().as_str().try_into()?;
            let blob_stream = match blob_store.get(&digest).await? {
                Some((_, stream)) => stream,
                None => return Err(Error::BlobNotFound(format!("{:?}", layer.digest()))),
            };
            let blob_bytes: BytesMut = blob_stream
                .try_collect()
                .await
                .map_err(|e| Error::StreamCollectFailed(format!("{e:?}")))?;

            let mut layer = Layer::default();
            layer.data = blob_bytes.into_iter().map(char::from).collect();
            layer.history = history;

            layers.push(Arc::new(Mutex::new(layer)));
        }

        let tags: Vec<String> = manifest_store
            .get_tags(manifest_ref)
            .await?
            .iter()
            .map(|bt| bt.name().to_owned())
            .collect();

        Ok(Image {
            manifest_ref: manifest_ref.into(),
            descriptor: Some(descriptor),
            os: image_config.os().to_owned(),
            architecture: image_config.architecture().to_owned(),
            artifact_type: manifest.artifact_type().clone(),
            subject: manifest.subject().clone(),
            layers,
            tags,
            config: Some(image_config),
            manifest: Some(manifest),
        })
    }

    pub async fn pull_index(&self, name: String, manifest_ref: &ManifestRef) -> Result<Index> {
        let manifest_store = self.get_manifest_store(&name).await;

        // pull manifest
        let manifest_stream = match manifest_store.get(manifest_ref).await? {
            Some((_, stream)) => stream,
            None => return Err(Error::ManifestNotFound(format!("{:?}", manifest_ref))),
        };
        let manifest_bytes: BytesMut = manifest_stream
            .try_collect()
            .await
            .map_err(|e| Error::StreamCollectFailed(format!("{e:?}")))?;
        let manifest: ImageIndex = serde_json::from_slice(&manifest_bytes)?;

        let manifest_descriptors: Vec<Descriptor> =
            manifest.manifests().iter().map(Clone::clone).collect();

        let refs = manifest_descriptors
            .iter()
            .map(|d| {
                let digest = d.digest().as_str().try_into()?;
                Ok(ManifestRef::Digest(digest))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut images = self.pull_images(&name, &refs).await?;

        let new_images = refs
            .into_iter()
            .map(|mref| {
                let image = images
                    .remove(&mref)
                    .ok_or(Error::PushedImageNotInPulledImages(mref))?;

                Ok(Arc::new(Mutex::new(image)))
            })
            .collect::<Result<Vec<_>>>()?;

        let tags: Vec<String> = manifest_store
            .get_tags(manifest_ref)
            .await?
            .iter()
            .map(|bt| bt.name().to_owned())
            .collect();

        Ok(Index {
            manifest_ref: manifest_ref.into(),
            manifests: new_images,
            artifact_type: manifest.artifact_type().clone(),
            subject: manifest.subject().clone(),
            index_manifest: Some(manifest),
            digest: Some(OciDigest::from(manifest_bytes.as_ref())),
            tags,
        })
    }

    pub async fn pull_images(
        &self,
        name: &str,
        refs: &Vec<ManifestRef>,
    ) -> Result<HashMap<ManifestRef, Image>> {
        let mut set = JoinSet::new();
        let mut pulled_images: HashMap<ManifestRef, Image> = HashMap::new();

        for manifest_ref in refs.iter() {
            let loader = self.clone();
            let manifest_ref = manifest_ref.clone();
            let name = name.to_string();
            set.spawn(async move {
                let image = loader.pull_image(name, &manifest_ref).await?;
                Ok(image)
            });
        }

        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(mut image)) => {
                    if let Some(mut image) = pulled_images.insert(image.manifest_ref(), image) {
                        tracing::warn!("image already found: {:?}", image.manifest_ref());
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(e @ tokio::task::JoinError { .. }) => return Err(e.into()),
            }
        }

        Ok(pulled_images)
    }

    pub async fn pull_indices(
        &self,
        name: &str,
        refs: Vec<ManifestRef>,
    ) -> Result<HashMap<ManifestRef, Index>> {
        let mut set = JoinSet::new();
        let mut pulled_images: HashMap<ManifestRef, Index> = HashMap::new();

        for manifest_ref in refs.iter() {
            let loader = self.clone();
            let manifest_ref = manifest_ref.clone();
            let name = name.to_string();
            set.spawn(async move {
                let index = loader.pull_index(name, &manifest_ref).await?;
                Ok(index)
            });
        }

        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(mut index)) => {
                    if let Some(mut index) = pulled_images.insert(index.manifest_ref(), index) {
                        tracing::warn!("index already found: {:?}", index.manifest_ref());
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(e @ tokio::task::JoinError { .. }) => return Err(e.into()),
            }
        }

        Ok(pulled_images)
    }
}
