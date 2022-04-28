use serde::Deserialize;

use aws_sdk_s3::{Client, Config, Credentials, Endpoint, Region};
use aws_types::credentials::{ProvideCredentials, SharedCredentialsProvider};
use http::Uri;
use hyper::body::Body;
use uuid::Uuid;

use crate::errors::Result;

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
            config: self,
            client: Client::from_conf(config),
        })
    }
}

pub struct S3 {
    config: *S3Config,
    client: Client,
}

impl S3 {
    pub async fn upload_blob(&self, digest: &str, body: Body) -> Result<()> {
        let _put_object_output = self
            .client
            .put_object()
            .key(digest)
            .body(body.into())
            .bucket(self.config.bucket_name.as_str())
            .send()
            .await?;
        Ok(())
    }
}
