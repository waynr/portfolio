use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    AddrParseError(#[from] std::net::AddrParseError),

    #[error("sqlx error")]
    SQLXError(#[from] sqlx::Error),
    #[error("sqlx migration error")]
    SQLXMigrateError(#[from] sqlx::migrate::MigrateError),
    #[error("sea-query error")]
    SeaQueryError(#[from] sea_query::error::Error),

    #[error("config deserialization error")]
    ConfigError(#[from] serde_yaml::Error),
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("http error")]
    HTTPError(#[from] http::Error),
    #[error("http invalid header name")]
    HTTPInvalidHeaderName(#[from] http::header::InvalidHeaderName),
    #[error("http invalid header value")]
    HTTPInvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
    #[error("{0}")]
    HyperError(#[from] hyper::Error),

    #[error("{0}")]
    ByteStreamError(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error("{0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    // #[error("aws sdk credentials error")]
    // AWSSDKCredentialsError(#[from] aws_types::credentials::CredentialsError),
    #[error("aws sdk put object error")]
    AWSSDKPutObjectError(
        #[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
    ),
    #[error("aws sdk get object error")]
    AWSSDKGetObjectError(
        #[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    ),
    #[error("aws sdk head object error")]
    AWSSDKHeadObjectError(
        #[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>,
    ),
    #[error("aws sdk copy object error")]
    AWSSDKCopyObjectError(
        #[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::copy_object::CopyObjectError>,
    ),
    #[error("aws sdk delete object error")]
    AWSSDKDeleteObjectError(
        #[from]
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_object::DeleteObjectError>,
    ),
    #[error("aws sdk create multipart upload error")]
    AWSSDKCreateMultiPartUploadError(
        #[from]
        aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError,
        >,
    ),
    #[error("aws sdk upload part error")]
    AWSSDKUploadPartError(
        #[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::upload_part::UploadPartError>,
    ),
    #[error("aws sdk complete multipart upload error")]
    AWSSDKCompleteMultipartUploadError(
        #[from]
        aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError,
        >,
    ),
    #[error("aws sdk abort multipart upload error")]
    AWSSDKAbortMultipartUploadError(
        #[from]
        aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError,
        >,
    ),
    #[error("aws sdk credentials error")]
    AWSSDKCredentialsError(#[from] aws_credential_types::provider::error::CredentialsError),

    #[error("failed to initiate chunked upload: {0}")]
    ObjectsFailedToInitiateChunkedUpload(&'static str),
    #[error("missing upload id for session: {0}")]
    ObjectsMissingUploadID(uuid::Uuid),

    #[error("error serializing to value")]
    SerdeJsonToValueError(#[from] serde_json::Error),

    // input validation errors
    #[error("invalid uuid")]
    InvalidUuid(#[from] uuid::Error),
    #[error("invalid digest: {0}")]
    InvalidDigest(String),
    #[error("unsupported digest algorithm: {0}")]
    UnsupportedDigestAlgorithm(String),

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),
    #[error("missing header: {0}")]
    MissingHeader(&'static str),
    #[error("missing path parameter: {0}")]
    MissingPathParameter(&'static str),
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(&'static str),

    // metadata errors
    #[error("PostgresMetadataTx already rolled back or committed")]
    PostgresMetadataTxInactive,

    // distribution error codes
    // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
    #[error("distribution spec error")]
    DistributionSpecError(DistributionErrorCode),
}

// TODO: need to generate JSON error body format as described in https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
#[derive(Debug)]
pub enum DistributionErrorCode {
    BlobUnknown = 1,         // blob unknown to registry
    BlobUploadInvalid = 2,   // blob upload invalid
    BlobUploadUnknown = 3,   // blob upload unknown to registry
    DigestInvalid = 4,       // provided digest did not match uploaded content
    ManifestBlobUnknown = 5, // manifest references a manifest or blob unknown to registry
    ManifestInvalid = 6,     // manifest invalid
    ManifestUnknown = 7,     // manifest unknown to registry
    NameInvalid = 8,         // invalid repository name
    NameUnknown = 9,         // repository name not known to registry
    SizeInvalid = 10,        // provided length did not match content length
    Unauthorized = 12,       // authentication required
    Denied = 13,             // request access to the resource is denied
    Unsupported = 14,        // the operation is unsupported
    TooManyRequests = 15,    // too many requests
    ContentReferenced = 99,  // content referenced elsewhere
}

impl DistributionErrorCode {
    fn status_code(&self) -> StatusCode {
        match self {
            DistributionErrorCode::BlobUnknown => StatusCode::NOT_FOUND,
            DistributionErrorCode::BlobUploadInvalid => StatusCode::RANGE_NOT_SATISFIABLE,
            DistributionErrorCode::BlobUploadUnknown => StatusCode::BAD_REQUEST,
            DistributionErrorCode::DigestInvalid => StatusCode::BAD_REQUEST,
            DistributionErrorCode::ManifestBlobUnknown => StatusCode::NOT_FOUND,
            DistributionErrorCode::ManifestInvalid => StatusCode::BAD_REQUEST,
            DistributionErrorCode::ManifestUnknown => StatusCode::NOT_FOUND,
            DistributionErrorCode::NameInvalid => StatusCode::BAD_REQUEST,
            DistributionErrorCode::NameUnknown => StatusCode::NOT_FOUND,
            DistributionErrorCode::SizeInvalid => StatusCode::BAD_REQUEST,
            DistributionErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
            DistributionErrorCode::Denied => StatusCode::FORBIDDEN,
            DistributionErrorCode::Unsupported => StatusCode::NOT_IMPLEMENTED,
            DistributionErrorCode::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
            DistributionErrorCode::ContentReferenced => StatusCode::CONFLICT,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::DistributionSpecError(dec) => (dec.status_code(), format!("{:?}", dec)),
            Error::InvalidUuid(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::InvalidDigest(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::MissingHeader(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::InvalidHeaderValue(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::MissingPathParameter(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            _ => {
                tracing::warn!("{:?}", self);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    String::from("something bad happened"),
                )
            }
        }
        .into_response()
    }
}
