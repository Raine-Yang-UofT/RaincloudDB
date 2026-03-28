use std::{fs, io};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use crate::types::CATALOG_FILE;


/// Database metadata
#[derive(Serialize, Deserialize)]
pub struct GlobalCatalogData {
    pub databases: HashSet<String>,
}

pub struct GlobalCatalog {
    dbms_root_dir: PathBuf,
    data: GlobalCatalogData,
}

impl GlobalCatalog {
    pub fn new(dbms_root_dir: &Path) -> Self {
        // load catalog data
        let catalog_path = dbms_root_dir.join(CATALOG_FILE);

        let data = match Self::load_catalog(&catalog_path) {
            Ok(data) => data,
            Err(_) => {
                // create new catalog file
                let _ = Self::create_empty_catalog(&catalog_path);
                GlobalCatalogData { databases: HashSet::new() }
            }
        };

        GlobalCatalog {
            dbms_root_dir: dbms_root_dir.to_path_buf(),
            data,
        }
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.dbms_root_dir.join(CATALOG_FILE)
    }

    fn load_catalog(path: &Path) -> io::Result<GlobalCatalogData> {
        let text = std::fs::read_to_string(path)?;
        let data = serde_json::from_str::<GlobalCatalogData>(&text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(data)
    }

    pub fn save_catalog(&self) -> io::Result<()> {
        let json = serde_json::to_string_pretty(&self.data)?;
        fs::write(self.catalog_path(), json)
    }

    fn create_empty_catalog(path: &Path) -> io::Result<()> {
        let empty = GlobalCatalogData { databases: HashSet::new() };
        let json = serde_json::to_string_pretty(&empty)?;
        fs::write(path, json)?;
        Ok(())
    }

    pub fn has_database(&self, name: &str) -> bool {
        self.data.databases.contains(&name.to_uppercase())
    }

    pub fn add_database(&mut self, name: String) {
        let name = name.to_uppercase();
        self.data.databases.insert(name);
        self.save_catalog().expect("Failure to update catalog during CREATE DATABASE");
    }

    pub fn remove_database(&mut self, name: &str) {
        let name = &name.to_uppercase();
        self.data.databases.remove(name);
        self.save_catalog().expect("Failed to update catalog during DROP DATABASE");
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.data.databases.iter().cloned().collect()
    }
}