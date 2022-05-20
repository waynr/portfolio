use serde::Deserialize;

use aws_sdk_s3::client::Builder;
use aws_sdk_s3::middleware::DefaultMiddleware;
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Config, Credentials, Endpoint, Region};
use aws_smithy_client::erase::DynMiddleware;
use aws_types::credentials::{ProvideCredentials, SharedCredentialsProvider};
use axum::body::StreamBody;
use http::Uri;
use hyper::body::Body;

use tower::layer::util::Stack;

use crate::{
    errors::{Error, Result},
    http::middleware::LogLayer,
    registry::{Chunk, UploadSession},
    OciDigest,
};

#[derive(Deserialize)]
pub struct S3Config {
    secret_key: String,
    access_key: String,
    hostname: String,
    bucket_name: String,
}

impl S3Config {
    pub async fn new_objects(&self) -> Result<S3> {
        let scp = SharedCredentialsProvider::new(
            Credentials::new(
                self.access_key.clone(),
                self.secret_key.clone(),
                None,
                None,
                "portfolio-hardcoded",
            )
            .provide_credentials()
            .await?,
        );
        let uri = Uri::builder()
            .scheme("https")
            .authority(self.hostname.as_str())
            .path_and_query("/")
            .build()?;
        let config = Config::builder()
            .region(Region::new("us-east-1"))
            .credentials_provider(scp)
            .endpoint_resolver(Endpoint::mutable(uri))
            .build();

        let middleware = Stack::new(LogLayer { target: "meow" }, DefaultMiddleware::new());
        let inner_client = Builder::new()
            .rustls()
            .middleware(DynMiddleware::new(middleware))
            .build();

        Ok(S3 {
            bucket_name: self.bucket_name.clone(),
            client: Client::with_config(inner_client, config),
        })
    }
}

pub struct S3 {
    bucket_name: String,
    client: Client,
}

impl S3 {
    pub async fn get_blob(&self, key: &OciDigest) -> Result<StreamBody<ByteStream>> {
        let get_object_output = self
            .client
            .get_object()
            .key(key)
            .bucket(&self.bucket_name)
            .send()
            .await?;

        Ok(StreamBody::new(get_object_output.body))
    }

    pub async fn blob_exists(&self, key: &OciDigest) -> Result<bool> {
        match self
            .client
            .head_object()
            .key(key)
            .bucket(&self.bucket_name)
            .send()
            .await
        {
            Err(e) => match e {
                aws_sdk_s3::types::SdkError::ServiceError {
                    err:
                        aws_sdk_s3::error::HeadObjectError {
                            kind: aws_sdk_s3::error::HeadObjectErrorKind::NotFound(_),
                            ..
                        },
                    ..
                } => Ok(false),
                _ => Err(Error::AWSSDKHeadObjectError(e)),
            },
            Ok(_) => Ok(true),
        }
    }

    pub async fn upload_blob(
        &self,
        key: &OciDigest,
        body: Body,
        content_length: u64,
    ) -> Result<()> {
        let _put_object_output = self
            .client
            .put_object()
            .key(key)
            .body(body.into())
            .content_length(content_length as i64)
            .bucket(&self.bucket_name)
            .send()
            .await?;
        Ok(())
    }

    pub async fn initiate_chunked_upload(&self, session: &mut UploadSession) -> Result<()> {
        let create_multipart_upload_output = self
            .client
            .create_multipart_upload()
            .key(session.uuid.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;

        if let Some(upload_id) = create_multipart_upload_output.upload_id {
            session.upload_id = Some(upload_id);
        } else {
            return Err(Error::ObjectsFailedToInitiateChunkedUpload(
                "missing upload id",
            ));
        }

        Ok(())
    }

    pub async fn upload_chunk(
        &self,
        session: &mut UploadSession,
        content_length: u64,
        body: Body,
    ) -> Result<Chunk> {
        // this state shouldn't be reachable, but if it is then consider it an error so we can
        // learn about it at runtime (rather than simply ignoring it)
        let upload_id = session
            .upload_id
            .clone()
            .ok_or_else(|| Error::ObjectsMissingUploadID(session.uuid))?;
        let upload_part_output = self
            .client
            .upload_part()
            .upload_id(upload_id)
            .part_number(session.chunk_number)
            .key(session.uuid.to_string())
            .body(body.into())
            .content_length(content_length as i64)
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let chunk = Chunk {
            e_tag: upload_part_output.e_tag,
            chunk_number: session.chunk_number,
        };
        session.chunk_number += 1;

        Ok(chunk)
    }

    pub async fn finalize_chunked_upload(
        &self,
        session: &UploadSession,
        chunks: Vec<Chunk>,
        dgst: &OciDigest,
    ) -> Result<()> {
        // this state shouldn't be reachable, but if it is then consider it an error so we can
        // learn about it at runtime (rather than simply ignoring it)
        let upload_id = session
            .upload_id
            .clone()
            .ok_or_else(|| Error::ObjectsMissingUploadID(session.uuid))?;

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
            .key(session.uuid.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let copy_source = format!("{}/{}", &self.bucket_name, session.uuid);
        let _copy_object_output = self
            .client
            .copy_object()
            .copy_source(copy_source)
            .key(dgst)
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let _delete_object_output = self
            .client
            .delete_object()
            .key(session.uuid.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;
        Ok(())
    }

    pub async fn abort_chunked_upload(&self, session: &UploadSession) -> Result<()> {
        let upload_id = session
            .upload_id
            .clone()
            .ok_or_else(|| Error::ObjectsMissingUploadID(session.uuid))?;
        let _complete_multipart_upload_output = self
            .client
            .abort_multipart_upload()
            .upload_id(upload_id)
            .key(session.uuid.to_string())
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
