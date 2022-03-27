use serde::Deserialize;

use aws_sdk_s3::{Client, Config, Credentials, Endpoint, Region};
use aws_types::credentials::{ProvideCredentials, SharedCredentialsProvider};
use http::Uri;

use crate::errors::Result;

pub struct S3 {
    client: Client,
}

#[derive(Deserialize)]
pub struct S3Config {
    secret_key: String,
    access_key: String,
    endpoint: String,
}

impl S3Config {
    pub async fn new_objects(&'static self) -> Result<S3> {
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
        let config = Config::builder()
            .region(Region::new("us-east-1"))
            .credentials_provider(scp)
            .endpoint_resolver(Endpoint::mutable(Uri::from_static(self.endpoint.as_str())))
            .build();

        Ok(S3 { client: Client::from_conf(config) })
    }
}
