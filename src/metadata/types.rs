use uuid::Uuid;

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
    pub(crate) id: Uuid,
    pub repository_id: Uuid,
}

pub struct Manifest {
    pub(crate) id: Uuid,
    pub repository_id: Uuid,
}
