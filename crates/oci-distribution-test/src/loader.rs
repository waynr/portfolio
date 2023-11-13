use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use hyper::body::Body;
use tokio::task::JoinSet;

use portfolio_core::registry::BlobStore;
use portfolio_core::registry::BoxedRepositoryStoreManager;
use portfolio_core::registry::ManifestSpec;
use portfolio_core::registry::ManifestStore;
use portfolio_core::registry::{BoxedRepositoryStore, RepositoryStoreManager};
use portfolio_core::OciDigest;

pub use super::errors::Result;
use super::{Image, Index};

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

    async fn upload_layer(
        blob_store: ArcBlobStore,
        repo_name: String,
        layer: Arc<Mutex<Layer>>,
    ) -> Result<()> {
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
        repo_name: String,
        image: Arc<Mutex<Image>>,
    ) -> Result<()> {
        tracing::info!("pushing image: {:?}", image.lock().unwrap().manifest_ref());

        let mut set = JoinSet::new();
        for layer in &image.lock().unwrap().layers {
            let blob_store = blob_store.clone();
            let name = repo_name.to_string();
            let layer = layer.clone();
            set.spawn(async move { Self::upload_layer(blob_store, name, layer).await });
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
        &self,
        repo_name: &str,
        images: Vec<Arc<Mutex<Image>>>,
    ) -> Result<()> {
        let manifest_store = self.get_manifest_store(repo_name).await;
        let blob_store = self.get_blob_store(repo_name).await;

        let mut set = JoinSet::new();
        for image in images {
            let manifest_store = manifest_store.clone();
            let blob_store = blob_store.clone();
            let image = image.clone();
            let repo_name = repo_name.to_string();
            set.spawn(async move {
                Self::upload_image(manifest_store, blob_store, repo_name, image).await
            });
        }
        Ok(())
    }

    pub async fn upload_indices(&self, repo_name: &str, mut indices: Vec<Index>) -> Result<()> {
        let manifest_store = self.get_manifest_store(repo_name).await;

        for index in &mut indices {
            self.upload_images(repo_name, index.manifests.clone())
                .await?;
            let manifest = index.manifest();
            let manifest_bytes = serde_json::to_vec(&manifest)?;

            tracing::info!("pushing index manifest: {:?}", index.manifest_ref());
            manifest_store
                .put(
                    &index.manifest_ref(),
                    &ManifestSpec::Index(manifest),
                    Bytes::from(manifest_bytes),
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;

    use anyhow::Result;
    use portfolio_backend_postgres::PgRepositoryConfig;
    use serde::Deserialize;

    use super::super::testdata;
    use super::*;

    #[derive(Clone, Deserialize)]
    #[serde(tag = "type")]
    pub enum RepositoryBackend {
        Postgres(PgRepositoryConfig),
    }

    #[derive(Clone, Deserialize)]
    pub struct Config {
        pub backend: RepositoryBackend,
    }

    async fn init_backend(path: PathBuf) -> Result<RepositoryLoader> {
        let mut dev_config = File::open(path)?;
        let mut s = String::new();
        dev_config.read_to_string(&mut s)?;
        let config: Config = serde_yaml::from_str(&s)?;

        match config.backend {
            RepositoryBackend::Postgres(cfg) => {
                let manager = cfg.get_manager().await?;
                Ok(RepositoryLoader::new(Box::new(manager)))
            }
        }
    }

    #[tokio::test]
    async fn push_and_pull_image() -> Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter(
                //"oci_distribution_test=trace,portfolio_core=debug,sqlx::query=debug,portfolio_backend_postgres=debug",
                "oci_distribution_test=trace,portfolio_core=debug,portfolio_backend_postgres=debug",
            )
            .with_test_writer()
            .with_target(true)
            .compact()
            .init();
        let loader = init_backend(PathBuf::from("../../dev-config-linode.yml")).await?;

        let basic_images = testdata::BASIC_IMAGES
            .clone()
            .into_iter()
            .map(Mutex::new)
            .map(Arc::new)
            .collect();
        let basic_indices = testdata::BASIC_INDEXES.clone();

        let mut set = JoinSet::new();
        {
            let loader = loader.clone();
            set.spawn(async move { loader.upload_images("testrepo", basic_images).await });
        }
        {
            let loader = loader.clone();
            set.spawn(async move { loader.upload_indices("testrepo", basic_indices).await });
        }
        while let Some(res) = set.join_next().await {
            match res {
                Err(e) => return Err(e.into()),
                _ => (),
            }
        }

        Ok(())
    }
}
