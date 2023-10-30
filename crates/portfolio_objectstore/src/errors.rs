//! ObjectStore errors

use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

/// General purpose [`super::ObjectStore`] error handling.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http error")]
    HTTPError(#[from] http::Error),

    #[error("{0}")]
    ByteStreamError(#[from] aws_sdk_s3::primitives::ByteStreamError),

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

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),

    // metadata errors
    #[error("PostgresMetadataTx already rolled back or committed")]
    PostgresMetadataTxInactive,

    #[error("key error: {0}")]
    KeyError(#[from] KeyError),
}

/// Error type used when parsing [`super::Key`] from [`std::path::PathBuf`].
#[derive(thiserror::Error, Debug)]
pub enum KeyError {
    #[error("prefix not allowed")]
    PrefixNotAllowed,

    #[error("root dir not allowed")]
    RootDirNotAllowed,

    #[error("current dir (`.`) not allowed")]
    CurDirNotAllowed,

    #[error("parent dir (`..`) not allowed")]
    ParentDirNotAllowed,

    #[error("path components must be valid unicode")]
    PathComponentsMustBeValidUnicode,

    #[error("path components must match regex: {0}")]
    PathComponentsMustMatchRegex(String),
}
