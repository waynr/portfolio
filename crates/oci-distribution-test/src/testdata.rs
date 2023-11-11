use lazy_static::lazy_static;

use super::Image;
use super::Index;

lazy_static! {
    pub static ref BASIC_IMAGES: Vec<Image> = initialize_basic_images();
    pub static ref BASIC_INDEXES: Vec<Index> = initialize_basic_indices();
    pub static ref EMPTY_IMAGE: Image = Default::default();
    pub static ref EMPTY_INDEX: Index = Default::default();
}

fn initialize_basic_images() -> Vec<Image> {
    serde_yaml::from_str(
        r#"
- manifest_ref: !Tag meow
  config:
  os: linux
  architecture: amd64
  layers:
  - data: "layer 1"
    history:
      comment: "this layer created for testing purposes"
  - data: "layer 2"
    history:
      comment: "this layer created for testing purposes"
  - data: "layer 3"
    history:
      comment: "this layer created for testing purposes"
        "#,
    )
    .expect("expect valid image")
}

fn initialize_basic_indices() -> Vec<Index> {
    serde_yaml::from_str(
        r#"
- manifest_ref: Digest
  manifests:
    - config:
      manifest_ref: Digest
      os: linux
      architecture: amd64
      layers:
      - data: "layer 1"
        history:
          comment: "this layer created for testing purposes"
      - data: "layer 2"
        history:
          comment: "this layer created for testing purposes"
      - data: "layer 3"
        history:
          comment: "this layer created for testing purposes"
    - config:
      manifest_ref: Digest
      os: linux
      architecture: amd64
      layers:
      - data: "layer 1"
        history:
          comment: "this layer created for testing purposes"
      - data: "layer 2"
        history:
          comment: "this layer created for testing purposes"
      - data: "layer 3"
        history:
          comment: "this layer created for testing purposes"
      - data: "layer 4"
        history:
          comment: "this layer created for testing purposes"
    - config:
      manifest_ref: Digest
      os: linux
      architecture: amd64
      layers:
      - data: "layer 1"
        history:
          comment: "this layer created for testing purposes"
      - data: "layer 4"
        history:
          comment: "this layer created for testing purposes"
        "#,
    )
    .expect("expect valid image")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn validate_basic_images() {
        let images = initialize_basic_images();
        for mut image in images {
            let _cfg = image.config();
            let _manifest = image.manifest();
        }
    }

    #[test]
    fn validate_basic_indices() {
        let indices = initialize_basic_indices();
        for mut index in indices {
            let _index_manifest = index.manifest();
        }
    }

    #[test]
    fn validate_empty_image() {
        let mut image = EMPTY_IMAGE.clone();
        let _cfg = image.config();
        let _manifest = image.manifest();
    }

    #[test]
    fn validate_empty_index() {
        let mut index = EMPTY_INDEX.clone();
        let _manifest = index.manifest();
    }
}
