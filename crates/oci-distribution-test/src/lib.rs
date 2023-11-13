use std::sync::Arc;
use std::sync::Mutex;

use oci_spec::image::{
    Arch, Descriptor, DescriptorBuilder, History, ImageConfiguration, ImageConfigurationBuilder,
    ImageIndex, ImageIndexBuilder, ImageManifest, ImageManifestBuilder, MediaType, Os,
    RootFsBuilder,
};
use serde::{Deserialize, Serialize};

use portfolio_core::registry::ManifestRef;
use portfolio_core::OciDigest;

mod errors;
mod loader;
mod testdata;
pub use errors::Result;

pub use loader::RepositoryLoader;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Layer {
    pub data: String,
    pub history: Option<History>,

    descriptor: Option<Descriptor>,
}

impl Layer {
    pub fn descriptor(&mut self) -> Descriptor {
        if let Some(d) = &self.descriptor {
            return d.clone();
        }

        let digest = OciDigest::from(self.data.as_ref());
        let descriptor = DescriptorBuilder::default()
            .media_type(MediaType::ImageLayer)
            .digest(digest)
            .size(self.data.len() as i64)
            .build()
            .expect("must set all required fields for descriptor");

        self.descriptor = Some(descriptor.clone());
        descriptor
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub enum ManifestReference {
    #[default]
    Digest,
    Tag(String),
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Image {
    manifest_ref: ManifestReference,
    pub os: Os,
    pub architecture: Arch,
    pub layers: Vec<Arc<Mutex<Layer>>>,
    pub artifact_type: Option<MediaType>,
    pub subject: Option<Descriptor>,

    #[serde(skip)]
    config: Option<ImageConfiguration>,
    #[serde(skip)]
    manifest: Option<ImageManifest>,
    #[serde(skip)]
    descriptor: Option<Descriptor>,
}

impl Image {
    pub fn config(&mut self) -> ImageConfiguration {
        if let Some(c) = &self.config {
            return c.clone();
        }

        let digests = self
            .layers
            .iter_mut()
            .map(|l| l.lock().unwrap().descriptor().digest().clone())
            .collect::<Vec<String>>();
        let histories: Vec<History> = self
            .layers
            .iter()
            .map(|l| l.lock().unwrap().history.clone().unwrap_or_else(Default::default))
            .collect();
        let rootfs = RootFsBuilder::default()
            .typ("layers".to_string())
            .diff_ids(digests)
            .build()
            .expect("must include all required fields for rootfs");
        let builder = ImageConfigurationBuilder::default()
            .os(self.os.clone())
            .architecture(self.architecture.clone())
            .history(histories)
            .rootfs(rootfs);

        let config = builder
            .build()
            .expect("must set all required fields for image configuration");

        self.config = Some(config.clone());
        config
    }

    pub fn digest(&mut self) -> OciDigest {
        self.descriptor()
            .digest()
            .as_str()
            .try_into()
            .expect("digests created by this library should always be correctly formed")
    }

    pub fn manifest(&mut self) -> ImageManifest {
        if let Some(m) = &self.manifest {
            return m.clone();
        }

        let config_bytes = serde_json::to_vec(&self.config())
            .expect("properly initialized ImageConfiguration should not fail to serialize");
        let config_digest = OciDigest::from(config_bytes.as_slice());
        let config_descriptor = DescriptorBuilder::default()
            .media_type(MediaType::ImageManifest)
            .digest(config_digest)
            .size(config_bytes.len() as i64)
            .build()
            .expect("must set all required fields for descriptor");

        let layer_descriptors = self
            .layers
            .iter_mut()
            .map(|l| l.lock().unwrap().descriptor().clone())
            .collect::<Vec<Descriptor>>();

        let mut manifest_builder = ImageManifestBuilder::default()
            .schema_version(2u32)
            .media_type(MediaType::ImageManifest)
            .layers(layer_descriptors)
            .config(config_descriptor);

        if let Some(ref artifact_type) = self.artifact_type {
            manifest_builder = manifest_builder.artifact_type(artifact_type.clone());
        }

        if let Some(ref subject) = self.subject {
            manifest_builder = manifest_builder.subject(subject.clone());
        }

        let manifest = manifest_builder
            .build()
            .expect("must set all required fields for image manifest");

        self.manifest = Some(manifest.clone());

        manifest
    }

    pub fn manifest_ref(&mut self) -> ManifestRef {
        match &self.manifest_ref {
            ManifestReference::Digest => ManifestRef::Digest(self.digest()),
            ManifestReference::Tag(name) => ManifestRef::Tag(name.clone()),
        }
    }

    pub fn descriptor(&mut self) -> Descriptor {
        if let Some(d) = &self.descriptor {
            return d.clone();
        }

        let config_bytes =
            serde_json::to_vec(&self.manifest()).expect("ImageManifest should be properly formed");
        let digest = OciDigest::from(config_bytes.as_slice());

        let descriptor = DescriptorBuilder::default()
            .media_type(MediaType::ImageLayer)
            .digest(String::from(&digest).as_str())
            .size(config_bytes.len() as i64)
            .build()
            .expect("must set all required fields for descriptor");

        self.descriptor = Some(descriptor.clone());
        descriptor
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Index {
    manifest_ref: ManifestReference,
    pub manifests: Vec<Arc<Mutex<Image>>>,
    pub artifact_type: Option<MediaType>,
    pub subject: Option<Descriptor>,

    #[serde(skip)]
    index_manifest: Option<ImageIndex>,
    #[serde(skip)]
    digest: Option<OciDigest>,
}

impl Index {
    pub fn manifest(&mut self) -> ImageIndex {
        if let Some(m) = &self.index_manifest {
            return m.clone();
        }

        let manifest_descriptors = self
            .manifests
            .iter_mut()
            .map(|m| m.lock().unwrap().descriptor())
            .collect::<Vec<Descriptor>>();

        let mut manifest_builder = ImageIndexBuilder::default()
            .schema_version(2u32)
            .media_type(MediaType::ImageIndex)
            .manifests(manifest_descriptors);

        if let Some(ref artifact_type) = self.artifact_type {
            manifest_builder = manifest_builder.artifact_type(artifact_type.clone());
        }

        if let Some(ref subject) = self.subject {
            manifest_builder = manifest_builder.subject(subject.clone());
        }

        let index_manifest = manifest_builder
            .build()
            .expect("must set all required fields for image manifest");

        self.index_manifest = Some(index_manifest.clone());

        index_manifest
    }

    pub fn digest(&mut self) -> OciDigest {
        if let Some(digest) = &self.digest {
            return digest.clone();
        }

        let config_bytes =
            serde_json::to_vec(&self.manifest()).expect("ImageIndex should be properly formed");
        let digest = OciDigest::from(config_bytes.as_slice());

        self.digest = Some(digest.clone());
        digest
    }

    pub fn manifest_ref(&mut self) -> ManifestRef {
        match &self.manifest_ref {
            ManifestReference::Digest => ManifestRef::Digest(self.digest()),
            ManifestReference::Tag(name) => ManifestRef::Tag(name.clone()),
        }
    }
}
