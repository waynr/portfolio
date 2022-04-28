use axum::response::{IntoResponse, Response};
use axum::http::{StatusCode};
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

    #[error("error serializing to value")]
    SerdeJsonToValueError(#[from] serde_json::Error),

    // input validation errors
    #[error("invalid uuid")]
    InvalidUuid(#[from] uuid::Error),
    #[error("invalid digest: {0}")]
    InvalidDigest(String),

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),
    #[error("missing header: {0}")]
    MissingHeader(&'static str),
    #[error("missing path parameter: {0}")]
    MissingPathParameter(&'static str),

    // distribution error codes
    // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
    #[error("distribution spec error")]
    DistributionSpecError(DistributionErrorCode),
}

#[derive(Debug)]
pub enum DistributionErrorCode {
    BlobUploadUnknown = 3,
}

impl DistributionErrorCode {
    fn status_code(&self) -> StatusCode {
        match self {
            DistributionErrorCode::BlobUploadUnknown => StatusCode::BAD_REQUEST,
            _ => StatusCode::NOT_FOUND,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::DistributionSpecError(dec) => {
                (dec.status_code(), format!("{:?}", dec))
            },
            Error::InvalidUuid(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self))
            },
            Error::InvalidDigest(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self))
            },
            Error::MissingHeader(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self))
            },
            Error::MissingPathParameter(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self))
            },
            _ => {
                (StatusCode::INTERNAL_SERVER_ERROR, String::from("something bad happened"))
            }
        }.into_response()
    }
}
