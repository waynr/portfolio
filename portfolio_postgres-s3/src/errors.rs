use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("portfolio error: {0}")]
    PortfolioError(String),

    #[error("sqlx error")]
    SQLXError(#[from] sqlx::Error),
    #[error("sqlx migration error")]
    SQLXMigrateError(#[from] sqlx::migrate::MigrateError),
    #[error("sea-query error")]
    SeaQueryError(#[from] sea_query::error::Error),

    #[error("http error")]
    HTTPError(#[from] http::Error),

    #[error("{0}")]
    ByteStreamError(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error("{0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

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

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),

    // metadata errors
    #[error("PostgresMetadataTx already rolled back or committed")]
    PostgresMetadataTxInactive,

    // distribution error codes
    // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
    #[error("distribution spec error")]
    DistributionSpecError(portfolio::errors::DistributionErrorCode),
}

impl From<portfolio::errors::Error> for Error {
    fn from(e: portfolio::errors::Error) -> Error {
        Error::PortfolioError(format!("{}", e))
    }
}

impl From<Error> for portfolio::errors::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::DistributionSpecError(c) => portfolio::errors::Error::DistributionSpecError(c),
            _ => portfolio::errors::Error::BackendError(Box::new(e)),
        }
    }
}
