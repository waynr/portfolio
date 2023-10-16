use crate::{
    errors::Result, metadata::PostgresMetadataPool, objects::ObjectStore, repositories::Repository,
};

#[derive(Clone)]
pub struct Portfolio<O>
where
    O: ObjectStore,
{
    objects: O,
    metadata: PostgresMetadataPool,
}

impl<O: ObjectStore> Portfolio<O> {
    pub fn new(objects: O, metadata: PostgresMetadataPool) -> Self {
        Self { objects, metadata }
    }

    pub async fn get_repository(&self, name: &str) -> Result<Option<Repository<O>>> {
        Repository::get(name, self.metadata.clone(), self.objects.clone()).await
    }

    pub async fn insert_repository(&self, name: &str) -> Result<Repository<O>> {
        Ok(Repository::get_or_insert(name, self.metadata.clone(), self.objects.clone()).await?)
    }
}
