#![allow(dead_code)]
use std::sync::Arc;
use std::sync::Mutex;

use portfolio_core::registry::ManifestRef;

use super::errors::{Error, Result};
use super::loader::RepositoryLoader;
use super::Image;
use super::Index;

pub struct RepositoryTester {
    loader: RepositoryLoader,
}

impl RepositoryTester {
    pub fn new(loader: RepositoryLoader) -> Self {
        Self { loader }
    }
}

pub fn assert_images_eq(pushed: &Image, pulled: &Image) {
    assert_eq!(pushed.os, pulled.os);
    assert_eq!(pushed.architecture, pulled.architecture);
    assert_eq!(pushed.artifact_type, pulled.artifact_type);
    assert_eq!(pushed.subject, pulled.subject);
    assert_eq!(pushed.config, pulled.config);
    assert_eq!(pushed.manifest, pulled.manifest);
    assert_eq!(pushed.descriptor, pulled.descriptor);
    assert_eq!(pushed.layers.len(), pulled.layers.len());
    let zipped = pushed.layers.iter().zip(pulled.layers.iter());
    for (pushed_layer, pulled_layer) in zipped {
        let pushed_layer_mg = pushed_layer.lock().unwrap();
        let pulled_layer_mg = pulled_layer.lock().unwrap();
        assert_eq!(&pushed_layer_mg.data, &pulled_layer_mg.data);
        assert_eq!(pushed_layer_mg.history, pulled_layer_mg.history);
    }
}

pub fn assert_indices_eq(pushed: &Index, pulled: &Index) {
    assert_eq!(pushed.artifact_type, pulled.artifact_type);
    assert_eq!(pushed.subject, pulled.subject);
    assert_eq!(pushed.index_manifest, pulled.index_manifest);
    assert_eq!(pushed.digest, pulled.digest);
    assert_eq!(pushed.manifests.len(), pulled.manifests.len());
    let zipped = pushed.manifests.iter().zip(pulled.manifests.iter());
    for (pushed_image, pulled_image) in zipped {
        assert_images_eq(&pushed_image.lock().unwrap(), &pulled_image.lock().unwrap());
    }
}

impl RepositoryTester {
    pub async fn push_and_pull_images(&self, images: Vec<Image>) -> Result<()> {
        let images = images
            .into_iter()
            .map(Mutex::new)
            .map(Arc::new)
            .collect::<Vec<_>>();

        self.loader
            .clone()
            .upload_images("testrepo".to_string(), images.clone())
            .await?;

        let refs = images
            .iter()
            .map(|i| i.lock().unwrap().manifest_ref())
            .collect();
        let pulled_images = self.loader.pull_images("testrepo", &refs).await?;

        for image in images {
            let mut image_mg = image.lock().unwrap();
            let manifest_ref = image_mg.manifest_ref();
            if let Some(pulled_image) = pulled_images.get(&manifest_ref) {
                assert_images_eq(&image_mg, pulled_image);
                if let ManifestRef::Tag(name) = manifest_ref {
                    assert!(pulled_image.tags.contains(&name));
                }
            } else {
                return Err(Error::PushedImageNotInPulledImages(manifest_ref));
            }
        }

        Ok(())
    }

    pub async fn push_and_pull_indices(&self, indices: Vec<Index>) -> Result<()> {
        let indicies = indices
            .into_iter()
            .map(Mutex::new)
            .map(Arc::new)
            .collect::<Vec<_>>();

        self.loader
            .upload_indices("testrepo".to_string(), indicies.clone())
            .await?;

        let refs = indicies
            .iter()
            .map(|i| i.lock().unwrap().manifest_ref())
            .collect();
        let pulled_indices = self.loader.pull_indices("testrepo", refs).await?;

        for index in indicies {
            let mut index_mg = index.lock().unwrap();
            let manifest_ref = index_mg.manifest_ref();
            if let Some(pulled_index) = pulled_indices.get(&manifest_ref) {
                assert_indices_eq(&index_mg, pulled_index);
                if let ManifestRef::Tag(name) = manifest_ref {
                    assert!(pulled_index.tags.contains(&name));
                }
            } else {
                return Err(Error::PushedIndexNotInPulledIndices(manifest_ref));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;
    use std::sync::Once;

    use anyhow::Result;
    use portfolio_backend_postgres::PgRepositoryConfig;
    use serde::Deserialize;

    use super::super::testdata;
    use super::*;

    static INIT: Once = Once::new();

    fn init() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
            .with_env_filter(
                //"oci_distribution_test=trace,portfolio_core=debug,sqlx::query=debug,portfolio_backend_postgres=debug",
                "oci_distribution_test=trace,portfolio_core=debug,portfolio_backend_postgres=debug",
            )
            .with_test_writer()
            .with_target(true)
            .compact()
            .init();
        });
    }

    #[derive(Clone, Deserialize)]
    #[serde(tag = "type")]
    pub enum RepositoryBackend {
        Postgres(PgRepositoryConfig),
    }

    #[derive(Clone, Deserialize)]
    pub struct Config {
        pub backend: RepositoryBackend,
    }

    async fn init_backend(path: PathBuf) -> Result<RepositoryTester> {
        init();

        let mut dev_config = File::open(path)?;
        let mut s = String::new();
        dev_config.read_to_string(&mut s)?;
        let config: Config = serde_yaml::from_str(&s)?;

        match config.backend {
            RepositoryBackend::Postgres(cfg) => {
                let manager = cfg.get_manager().await?;
                Ok(RepositoryTester::new(RepositoryLoader::new(Box::new(
                    manager,
                ))))
            }
        }
    }

    #[tokio::test]
    async fn push_and_pull_image() -> Result<()> {
        let tester = init_backend(PathBuf::from("../../dev-config-linode.yml")).await?;
        let basic_images = testdata::BASIC_IMAGES.clone();

        tester.push_and_pull_images(basic_images).await?;

        Ok(())
    }

    #[tokio::test]
    pub async fn push_and_pull_index() -> Result<()> {
        let tester = init_backend(PathBuf::from("../../dev-config-linode.yml")).await?;
        let basic_indices = testdata::BASIC_INDEXES.clone();

        tester.push_and_pull_indices(basic_indices).await?;

        Ok(())
    }
}
