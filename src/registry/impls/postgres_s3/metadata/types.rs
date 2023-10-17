use sea_query::Iden;

#[derive(Iden)]
pub enum Repositories {
    Table,
    Id,
    RegistryId,
    Name,
}

#[derive(Iden)]
pub enum Blobs {
    Table,
    Id,
    RegistryId,
    Digest,
    BytesOnDisk,
}

#[derive(Iden)]
pub enum Tags {
    Table,
    Id,
    RepositoryId,
    ManifestId,
    Name,
}

#[derive(Iden)]
pub enum Manifests {
    Table,
    Id,
    RegistryId,
    BlobId,
    MediaType,
    ArtifactType,
    RepositoryId,
    Digest,
    Subject,
}

#[derive(Iden)]
pub enum Layers {
    Table,
    Manifest,
    Blob,
}

#[derive(Iden)]
pub enum IndexManifests {
    Table,
    ParentManifest,
    ChildManifest,
}
