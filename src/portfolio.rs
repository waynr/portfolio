use crate::{
    errors::Result, metadata::PostgresMetadataPool, objects::ObjectStore,
    registries::Registry,
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
        Self {
            objects,
            metadata,
        }
    }

    pub async fn get_registry(&self, name: &str) -> Result<Registry<O>> {
        Registry::new(name, self.metadata.clone(), self.objects.clone()).await
    }
}
