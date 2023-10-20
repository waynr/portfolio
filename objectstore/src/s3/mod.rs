use async_trait::async_trait;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::ByteStreamError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use http::{StatusCode, Uri};
use hyper::body::Body;
use serde::Deserialize;
use uuid::Uuid;

use super::Chunk;

pub(crate) mod logging;
use super::errors::{Error, Result};
use super::s3::logging::LoggingInterceptor;
use super::ObjectStore;

#[derive(Clone, Deserialize)]
pub struct S3Config {
    secret_key: String,
    access_key: String,
    hostname: String,
    bucket_name: String,
    region: String,
}

impl S3Config {
    pub async fn new_objects(&self) -> Result<S3> {
        let scp = SharedCredentialsProvider::new(
            Credentials::new(
                self.access_key.clone(),
                self.secret_key.clone(),
                None,
                None,
                "portfolio",
            )
            .provide_credentials()
            .await?,
        );

        let uri = Uri::builder()
            .scheme("https")
            .authority(self.hostname.as_str())
            .path_and_query("/")
            .build()?;

        let sdk_config = aws_config::load_from_env().await;

        let config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .region(Region::new(self.region.clone()))
            .credentials_provider(scp)
            .endpoint_url(uri.to_string())
            .interceptor(LoggingInterceptor)
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(config);

        Ok(S3 {
            bucket_name: self.bucket_name.clone(),
            client: s3_client,
        })
    }
}

#[derive(Clone)]
pub struct S3 {
    bucket_name: String,
    client: Client,
}

#[async_trait]
impl ObjectStore for S3 {
    type Error = Error;
    type ObjectStreamError = ByteStreamError;
    type Object = ByteStream;

    async fn get_blob(&self, key: &Uuid) -> Result<Self::Object> {
        let get_object_output = self
            .client
            .get_object()
            .key(key.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;

        Ok(get_object_output.body)
    }

    async fn blob_exists(&self, key: &Uuid) -> Result<bool> {
        match self
            .client
            .head_object()
            .key(key.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await
        {
            Err(SdkError::ServiceError(e)) => {
                let http = e.raw();
                match http.status() {
                    StatusCode::NOT_FOUND => Ok(false),
                    _ => Err(SdkError::ServiceError(e).into()),
                }
            }
            Err(e) => Err(Error::AWSSDKHeadObjectError(e)),
            Ok(_) => Ok(true),
        }
    }

    async fn upload_blob(&self, key: &Uuid, body: Body, content_length: u64) -> Result<()> {
        let _put_object_output = self
            .client
            .put_object()
            .key(key.to_string())
            .body(body.into())
            .content_length(content_length as i64)
            .bucket(&self.bucket_name)
            .send()
            .await?;
        Ok(())
    }

    async fn delete_blob(&self, key: &Uuid) -> Result<()> {
        self.client
            .delete_object()
            .key(key.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;
        Ok(())
    }

    async fn initiate_chunked_upload(&self, session_uuid: &Uuid) -> Result<String> {
        let create_multipart_upload_output = self
            .client
            .create_multipart_upload()
            .key(session_uuid.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let upload_id = create_multipart_upload_output.upload_id.ok_or(
            Error::ObjectsFailedToInitiateChunkedUpload("missing upload id"),
        )?;

        Ok(upload_id)
    }

    async fn upload_chunk(
        &self,
        upload_id: &str,
        session_uuid: &Uuid,
        chunk_number: i32,
        content_length: u64,
        body: Body,
    ) -> Result<Chunk> {
        let upload_part_output = self
            .client
            .upload_part()
            .upload_id(upload_id)
            .part_number(chunk_number)
            .key(session_uuid.to_string())
            .body(body.into())
            .content_length(content_length as i64)
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let chunk = Chunk {
            e_tag: upload_part_output.e_tag,
            chunk_number,
        };

        Ok(chunk)
    }

    async fn finalize_chunked_upload(
        &self,
        upload_id: &str,
        session_uuid: &Uuid,
        chunks: Vec<Chunk>,
        key: &Uuid,
    ) -> Result<()> {
        let session_uuid = session_uuid.to_string();
        let key = key.to_string();

        let mut mpu = CompletedMultipartUpload::builder();
        for chunk in chunks {
            let mut pb = CompletedPart::builder();
            if let Some(e_tag) = &chunk.e_tag {
                pb = pb.e_tag(e_tag);
            }
            mpu = mpu.parts(pb.part_number(chunk.chunk_number).build());
        }
        let _complete_multipart_upload_output = self
            .client
            .complete_multipart_upload()
            .multipart_upload(mpu.build())
            .upload_id(upload_id)
            .key(&session_uuid)
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let copy_source = format!("{}/{}", &self.bucket_name, session_uuid);
        let _copy_object_output = self
            .client
            .copy_object()
            .copy_source(copy_source)
            .key(key)
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let _delete_object_output = self
            .client
            .delete_object()
            .key(&session_uuid)
            .bucket(&self.bucket_name)
            .send()
            .await?;
        Ok(())
    }

    async fn abort_chunked_upload(&self, upload_id: &str, session_uuid: &Uuid) -> Result<()> {
        let _complete_multipart_upload_output = self
            .client
            .abort_multipart_upload()
            .upload_id(upload_id)
            .key(session_uuid.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;
        // TODO: list parts to identify any lingering parts that may have been uploading during the
        // abort? the SDK docs suggest doing this, but i don't think it should be possible for a
        // given session's parts to still be uploading when we reach this abort so it should be
        // fine to leave it for now.

        Ok(())
    }
}
