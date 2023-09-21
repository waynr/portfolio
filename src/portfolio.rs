use crate::{
    errors::Result, metadata::PostgresMetadata, objects::ObjectStore,
    registries::Registry,
};

#[derive(Clone)]
pub struct Portfolio<O>
where
    O: ObjectStore,
{
    objects: O,
    metadata: PostgresMetadata,
}

impl<O: ObjectStore> Portfolio<O> {
    pub fn new(objects: O, metadata: PostgresMetadata) -> Self {
        Self {
            objects,
            metadata,
        }
    }

    pub async fn get_registry(&self, name: &str) -> Result<Registry<O>> {
        Registry::new(name, self.metadata.clone(), self.objects.clone()).await
    }
}
