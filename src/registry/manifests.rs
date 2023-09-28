use crate::{
    errors::Result,
    metadata::{ImageManifest, ManifestRef, PostgresMetadata, Repository},
    objects::ObjectStore,
};

pub struct ManifestStore<'r, O>
where
    O: ObjectStore,
{
    metadata: PostgresMetadata,
    objects: O,
    repository: &'r Repository,
}

impl<'r, O> ManifestStore<'r, O>
where
    O: ObjectStore,
{
    pub fn new(metadata: PostgresMetadata, objects: O, repository: &'r Repository) -> Self {
        Self {
            metadata,
            objects,
            repository,
        }
    }

    pub async fn get_manifest(&self, key: &ManifestRef) -> Result<Option<ImageManifest>> {
        if let Some(mut manifest) = self
            .metadata
            .get_manifest(&self.repository.registry_id, &self.repository.id, key)
            .await?
        {
            let body = self.objects.get_blob(&manifest.config_blob_id).await?;
            manifest.body = Some(body);
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }
}
