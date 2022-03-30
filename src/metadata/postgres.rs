use serde::Deserialize;
use sqlx::postgres::{Postgres, PgPoolOptions};
use sqlx::Pool;

use crate::errors::Result;

#[derive(Deserialize)]
pub struct PostgresConfig {
    connection_string: String,
}

impl PostgresConfig {
    pub async fn new_metadata(&self) -> Result<PostgresMetadata> {
        let pool = PgPoolOptions::new().
            connect(&self.connection_string).await?;
        Ok(PostgresMetadata {
            pool,
        })
    }
}

pub struct PostgresMetadata {
    pool: Pool<Postgres>,
}

impl PostgresMetadata {
    pub async fn insert_blob(&self, digest: &str) -> Result<()> {
        let conn = self.pool.acquire().await?;
        Ok(())
    }
}
