use std::sync::Arc;

use serde::Deserialize;

use super::ObjectStore;
use super::Result;

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    S3(super::s3::S3Config),
}

impl Config {
    pub async fn new_objects(&self) -> Result<Arc<dyn ObjectStore>> {
        match self {
            Self::S3(cfg) => Ok(Arc::new(cfg.new_objects().await?)),
        }
    }
}
