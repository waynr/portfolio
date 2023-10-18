use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct RepositoryDefinition {
    pub name: String,
}
