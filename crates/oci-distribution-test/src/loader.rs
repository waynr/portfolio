use bytes::Bytes;
use hyper::body::Body;

use portfolio_core::registry::BoxedRepositoryStore;
use portfolio_core::registry::BoxedRepositoryStoreManager;
use portfolio_core::registry::ManifestSpec;
use portfolio_core::OciDigest;

pub use super::errors::Result;
use super::{Image, Index};

pub struct RepositoryLoader {
    mgr: BoxedRepositoryStoreManager,
}

impl RepositoryLoader {
    pub fn new(mgr: BoxedRepositoryStoreManager) -> Self {
        Self { mgr }
    }

    pub async fn get_or_create_repo(&self, name: &str) -> Result<BoxedRepositoryStore> {
        if let Some(repo) = self.mgr.get(name).await? {
            Ok(repo)
        } else {
            Ok(self.mgr.create("repo_1").await?)
        }
    }

    pub async fn upload_images(&self, repo_name: &str, mut images: Vec<Image>) -> Result<()> {
        let repo_store = self.get_or_create_repo(repo_name).await?;

        let mut manifest_store = repo_store.get_manifest_store();
        let mut blob_store = repo_store.get_blob_store();

        for image in &mut images {
            tracing::info!("pushing image: {:?}", image.manifest_ref());

            for layer in &mut image.layers {
                tracing::info!(
                    "pushing image layer: {}\n with contents\n {}",
                    layer.descriptor().digest(),
                    layer.data,
                );
                blob_store
                    .put(
                        &layer.descriptor().digest().as_str().try_into()?,
                        layer.data.len() as u64,
                        Body::from(layer.data.clone()),
                    )
                    .await?;
            }

            let manifest = image.manifest();
            let digest = manifest.config().digest();
            tracing::info!("pushing image config: {}", digest,);

            let config = image.config();
            let config_bytes = serde_json::to_vec(&config)?;
            let oci_digest: OciDigest = digest.as_str().try_into()?;
            blob_store
                .put(
                    &oci_digest,
                    config_bytes.len() as u64,
                    Body::from(config_bytes),
                )
                .await?;

            let manifest_bytes = serde_json::to_vec(&manifest)?;

            tracing::info!("pushing image manifest: {:?}", image.manifest_ref());
            manifest_store
                .put(
                    &image.manifest_ref(),
                    &ManifestSpec::Image(manifest),
                    Bytes::from(manifest_bytes),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn upload_indices(&self, repo_name: &str, mut indices: Vec<Index>) -> Result<()> {
        let repo_store = self.get_or_create_repo(repo_name).await?;

        let mut manifest_store = repo_store.get_manifest_store();

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

    use super::*;
    use super::super::testdata;

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

        let basic_images = testdata::BASIC_IMAGES.clone();
        let basic_indices = testdata::BASIC_INDEXES.clone();

        loader.upload_images("testrepo", basic_images).await?;
        loader.upload_indices("testrepo", basic_indices).await?;

        Ok(())
    }
}
