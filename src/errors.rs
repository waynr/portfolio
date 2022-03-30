use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("sqlx error")]
    SQLXError(#[from] sqlx::Error),
    #[error("sqlx migration error")]
    SQLXMigrateError(#[from] sqlx::migrate::MigrateError),
    #[error("config deserialization error")]
    ConfigError(#[from] serde_yaml::Error),
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("aws sdk credentials error")]
    AWSSDKCredentialsError(#[from] aws_types::credentials::CredentialsError),
    #[error("aws sdk put object error")]
    AWSSDKPutObjectError(#[from] aws_sdk_s3::types::SdkError<aws_sdk_s3::error::PutObjectError>),
    #[error("http error")]
    HTTPError(#[from] http::Error),
    #[error("invalid digest: {0}")]
    InvalidDigest(String),
}
