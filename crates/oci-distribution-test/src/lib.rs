use bytes::Bytes;
use derive_builder::Builder;
use oci_spec::image::{
    Descriptor, DescriptorBuilder, History, ImageConfiguration, ImageManifest,
    ImageManifestBuilder, MediaType,
    ImageIndex, ImageIndexBuilder,
};

use portfolio_core::registry::RepositoryStoreManager;
use portfolio_core::OciDigest;

mod errors;
use errors::{Error, Result};

pub struct DistributionTester<RSM: RepositoryStoreManager> {
    mgr: RSM,
}

#[derive(Builder, Clone)]
#[builder(build_fn(skip))]
pub struct Layer {
    pub data: Bytes,
    pub history: Option<History>,

    #[builder(setter(skip))]
    pub descriptor: Descriptor,
}

impl LayerBuilder {
    pub fn build(self) -> Result<Layer> {
        let data = self.data.ok_or(Error::LayerBuilderError(
            "must include data to construct Layer".to_string(),
        ))?;
        let digest = OciDigest::from(data.as_ref());
        let descriptor = DescriptorBuilder::default()
            .media_type(MediaType::ImageLayer)
            .digest(digest)
            .size(data.len() as i64)
            .build()
            .expect("must set all required fields for descriptor");
        Ok(Layer {
            data,
            descriptor,
            history: self.history.flatten(),
        })
    }
}

#[derive(Builder, Clone)]
#[builder(build_fn(skip))]
pub struct Image {
    pub config: ImageConfiguration,
    pub layers: Vec<Layer>,

    // artifact_type and subject are duplicated in the ImageConfiguration, but put here for the
    // sake of generating a builder method to allow users to specify them for the ImageConfiguring
    // when building an image.
    #[allow(dead_code)]
    artifact_type: Option<MediaType>,
    #[allow(dead_code)]
    subject: Option<Descriptor>,

    #[builder(setter(skip))]
    pub manifest: ImageManifest,
}

impl ImageBuilder {
    pub fn build(self) -> Result<Image> {
        let config = self.config.ok_or(Error::ImageBuilderError(
            "must include image configuration to construct image".to_string(),
        ))?;
        let config_bytes = serde_json::to_vec(&config)?;
        let config_digest = OciDigest::from(config_bytes.as_slice());
        let config_descriptor = DescriptorBuilder::default()
            .media_type(MediaType::ImageManifest)
            .digest(config_digest)
            .size(config_bytes.len() as i64)
            .build()
            .expect("must set all required fields for descriptor");

        let layers = self.layers.unwrap_or_else(Vec::new);
        let layer_descriptors = layers
            .iter()
            .map(|l| l.descriptor.clone())
            .collect::<Vec<Descriptor>>();

        let artifact_type = self.artifact_type.flatten();
        let subject = self.subject.flatten();

        let mut manifest_builder = ImageManifestBuilder::default()
            .schema_version(2u32)
            .media_type(MediaType::ImageManifest)
            .layers(layer_descriptors)
            .config(config_descriptor);

        if let Some(ref artifact_type) = artifact_type {
            manifest_builder = manifest_builder.artifact_type(artifact_type.clone());
        }

        if let Some(ref subject) = subject {
            manifest_builder = manifest_builder.subject(subject.clone());
        }

        let manifest = manifest_builder
            .build()
            .expect("must set all required fields for image manifest");

        Ok(Image {
            config,
            manifest,
            layers,
            artifact_type,
            subject,
        })
    }
}

#[derive(Builder)]
#[builder(build_fn(skip))]
pub struct Index {
    pub manifests: Vec<Image>,

    // artifact_type and subject are duplicated in the ImageConfiguration, but put here for the
    // sake of generating a builder method to allow users to specify them for the ImageConfiguring
    // when building an image.
    #[allow(dead_code)]
    artifact_type: Option<MediaType>,
    #[allow(dead_code)]
    subject: Option<Descriptor>,

    #[builder(setter(skip))]
    pub index_manifest: ImageIndex,
}

impl IndexBuilder {
    pub fn build(self) -> Result<Index> {
        let manifests = self.manifests.unwrap_or_else(Vec::new);
        let manifest_descriptors = manifests
            .iter()
            .map(|m| m.manifest.config().clone())
            .collect::<Vec<Descriptor>>();

        let artifact_type = self.artifact_type.flatten();
        let subject = self.subject.flatten();

        let mut manifest_builder = ImageIndexBuilder::default()
            .schema_version(2u32)
            .media_type(MediaType::ImageIndex)
            .manifests(manifest_descriptors);

        if let Some(ref artifact_type) = artifact_type {
            manifest_builder = manifest_builder.artifact_type(artifact_type.clone());
        }

        if let Some(ref subject) = subject {
            manifest_builder = manifest_builder.subject(subject.clone());
        }

        let index_manifest = manifest_builder
            .build()
            .expect("must set all required fields for image manifest");

        Ok(Index {
            manifests,
            index_manifest,
            artifact_type,
            subject,
        })
    }
}

impl<RSM: RepositoryStoreManager> DistributionTester<RSM> {
    pub fn new(mgr: RSM) -> Self {
        Self { mgr }
    }

    pub async fn generate_basic_image(&self) -> Result<()> {
        self.mgr.create("repo_1").await.map_err(|e| e.into())?;
        Ok(())
    }
}
