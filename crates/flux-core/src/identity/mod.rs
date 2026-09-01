use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::path::PathBuf;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PeerId(Uuid);

impl PeerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn get_storage_path(profile: &str) -> PathBuf {
        let mut path = dirs::config_dir().unwrap_or(PathBuf::from("."));
        path.push("flux");
        if !profile.is_empty() {
            path.push(profile);
        }
        fs::create_dir_all(&path).ok();
        path.push("identity.json");
        path
    }

    pub fn load_or_generate(profile: &str) -> Self {
        let path = Self::get_storage_path(profile);
        if path.exists() {
            let data = fs::read_to_string(path).unwrap();
            serde_json::from_str(&data).unwrap_or_else(|_| Self::generate_and_save(profile))
        } else {
            Self::generate_and_save(profile)
        }
    }

    fn generate_and_save(profile: &str) -> Self {
        let new_id = Self::new();
        let path = Self::get_storage_path(profile);
        let data = serde_json::to_string(&new_id).unwrap();
        fs::write(path, data).unwrap();
        new_id
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
