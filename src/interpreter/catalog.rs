use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CatalogData {
    pub databases: Vec<String>,
}

pub struct Catalog {
    config_dir: String,
    databases: HashSet<String>,
}

impl Catalog {
    pub fn new(config_dir: &str) -> Self {
        // load catalog data
        let catalog_path = Path::new(config_dir).join("catalog.json");

        let databases = match Self::load_catalog(&catalog_path) {
            Ok(data) => data.databases.into_iter().collect(),
            Err(_) => {
                // catalog missing or corrupted → create fresh file
                let _ = Self::create_empty_catalog(&catalog_path);
                HashSet::new()
            }
        };

        Catalog {
            config_dir: config_dir.to_string(),
            databases,
        }
    }

    fn load_catalog(path: &Path) -> io::Result<CatalogData> {
        let text = std::fs::read_to_string(path)?;
        let data = serde_json::from_str::<CatalogData>(&text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(data)
    }

    pub fn save_catalog(&self) -> io::Result<()> {
        let path = Path::new(&self.config_dir).join("catalog.json");
        let data = serde_json::to_string_pretty(&CatalogData {
            databases: self.databases.iter().cloned().collect()
        })?;
        std::fs::write(path, data)
    }

    fn create_empty_catalog(path: &Path) -> io::Result<()> {
        let empty = CatalogData { databases: vec![] };
        let json = serde_json::to_string_pretty(&empty)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn has_database(&self, name: &str) -> bool {
        self.databases.contains(name)
    }

    pub fn add_database(&mut self, name: String) {
        self.databases.insert(name);
        self.save_catalog().expect("Failure to update catalog during CREATE DATABASE");
    }

    pub fn remove_database(&mut self, name: &str) {
        self.databases.remove(name);
        self.save_catalog().expect("Failed to update catalog during DROP DATABASE");
    }

    pub fn list_databases(&self) -> Vec<String> {
        let mut list: Vec<String> = self.databases.iter().cloned().collect();
        list.sort();
        list
    }
}