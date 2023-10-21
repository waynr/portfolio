use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub enum Config {
    S3Config(super::s3::S3Config),
}
