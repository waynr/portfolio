use uuid::Uuid;

#[derive(Clone)]
pub struct Registry {
    pub(crate) id: Uuid,
    pub name: String,
}

pub struct Repository {
    pub(crate) id: Uuid,
    pub registry_id: Uuid,
    pub name: String,
}

pub struct Blob {
    pub id: Uuid,
    pub registry_id: Uuid,
    pub digest: String,
}

pub struct Manifest {
    pub(crate) id: Uuid,
    pub repository_id: Uuid,
}
