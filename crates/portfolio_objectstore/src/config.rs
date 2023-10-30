//! ObjectStore configuration

use std::sync::Arc;

use serde::Deserialize;

use super::ObjectStore;
use super::Result;

/// Deserializable config type with constructor that returns [`Arc<dyn ObjectStore>`]
/// instances.
#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    S3(super::s3::S3Config),
}

impl Config {
    /// Constructs an instance of [`Arc<dyn ObjectStore>`] whose concrete type depends
    /// on which variant is present.
    pub async fn new_objects(&self) -> Result<Arc<dyn ObjectStore>> {
        match self {
            Self::S3(cfg) => Ok(Arc::new(cfg.new_objects().await?)),
        }
    }
}
