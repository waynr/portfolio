use std::sync::Arc;

use serde::Deserialize;

use super::ObjectStore;
use super::Result;

#[derive(Clone, Deserialize)]
pub enum Config {
    S3Config(super::s3::S3Config),
}

impl Config {
    pub async fn new_objects(&self) -> Result<Arc<dyn ObjectStore>> {
        match self {
            Self::S3Config(cfg) => Ok(Arc::new(cfg.new_objects().await?)),
        }
    }
}
