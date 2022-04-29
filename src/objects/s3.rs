use serde::Deserialize;

use aws_sdk_s3::{Client, Config, Credentials, Endpoint, Region};
use aws_types::credentials::{ProvideCredentials, SharedCredentialsProvider};
use http::Uri;
use hyper::body::Body;

use uuid::Uuid;

use crate::{
    errors::{Error, Result},
    objects::ChunkInfo,
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

        Ok(S3 {
            bucket_name: self.bucket_name.clone(),
            client: Client::from_conf(config),
        })
    }
}

pub struct S3 {
    bucket_name: String,
    client: Client,
}

impl S3 {
    pub async fn upload_blob(&self, digest: &str, body: Body) -> Result<()> {
        let _put_object_output = self
            .client
            .put_object()
            .key(digest)
            .body(body.into())
            .bucket(&self.bucket_name)
            .send()
            .await?;
        Ok(())
    }

    pub async fn initiate_chunked_upload(&self, session_uuid: &Uuid) -> Result<ChunkInfo> {
        let create_multipart_upload_output = self
            .client
            .create_multipart_upload()
            .key(session_uuid.to_string())
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let mut chunk_info = ChunkInfo::default();
        if let Some(upload_id) = create_multipart_upload_output.upload_id {
            chunk_info.upload_id = upload_id;
        } else {
            return Err(Error::ObjectsFailedToInitiateChunkedUpload(
                "missing upload id",
            ));
        }

        Ok(chunk_info)
    }

    pub async fn upload_chunk(&self, chunk: &mut ChunkInfo, body: Body) -> Result<()> {
        let _upload_part_output = self
            .client
            .upload_part()
            .upload_id(chunk.upload_id.clone())
            .part_number(chunk.part_number)
            .body(body.into())
            .bucket(&self.bucket_name)
            .send()
            .await?;

        chunk.part_number += 1;
        Ok(())
    }

    pub async fn finalize_chunked_upload(&self, digest: &str) -> Result<()> {
        let _complete_multipart_upload_output = self
            .client
            .complete_multipart_upload()
            .key(digest)
            .bucket(&self.bucket_name)
            .send()
            .await?;
        Ok(())
    }
}
