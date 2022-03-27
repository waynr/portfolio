use serde::Deserialize;

use crate::metadata;

#[derive(Deserialize)]
pub struct Config {
    pub metadata: MetadataBackend,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum MetadataBackend {
    Postgres(metadata::PostgresConfig),
}
