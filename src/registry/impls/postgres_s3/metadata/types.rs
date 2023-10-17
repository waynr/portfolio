use sea_query::Iden;

#[derive(Iden)]
pub enum Repositories {
    Table,
    Id,
    Name,
}

#[derive(Iden)]
pub enum Blobs {
    Table,
    Id,
    Digest,
    BytesOnDisk,
}

#[derive(Iden)]
pub enum Tags {
    Table,
    RepositoryId,
    ManifestId,
    Name,
}

#[derive(Iden)]
pub enum Manifests {
    Table,
    Id,
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
