use std::sync::Arc;

pub struct Registry {
    pub(crate) id: i64,
    pub name: String,
}

pub struct Repository {
    pub(crate) id: i64,
    pub registry: Registry,
    pub name: String,
}

pub struct Blob {
    pub(crate) id: i64,
    pub repository: Repository,
}

pub struct Manifest {
    pub(crate) id: i64,
    pub repository: Repository,

    pub blobs: Vec<Blob>,
}
