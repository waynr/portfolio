use crate::config::RepositoryDefinition;
use crate::errors::Result;
use crate::registry::RepositoryStoreManager;

#[derive(Clone)]
pub struct Portfolio<R>
where
    R: RepositoryStoreManager,
{
    manager: R,
}

impl<R: RepositoryStoreManager> Portfolio<R> {
    pub fn new(manager: R) -> Self {
        Self { manager }
    }

    pub async fn initialize_static_repositories(
        &self,
        repositories: Vec<RepositoryDefinition>,
    ) -> Result<()> {
        for repository_config in repositories {
            match self.get_repository(&repository_config.name).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    tracing::info!(
                        "static repository '{}' not found, inserting into DB",
                        repository_config.name,
                    );
                    self.insert_repository(&repository_config.name).await?
                }
                Err(e) => return Err(e),
            };
        }
        Ok(())
    }

    pub async fn get_repository(&self, name: &str) -> Result<Option<R::RepositoryStore>> {
        self.manager.get(name).await
    }

    pub async fn insert_repository(&self, name: &str) -> Result<R::RepositoryStore> {
        Ok(self.manager.create(name).await?)
    }
}
