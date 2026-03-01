use std::{fs, io};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use crate::compiler::ast::{ColumnDef};
use crate::types::{PageId, CATALOG_FILE};

/// Table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub column_index: HashMap<String, usize>,
    pub first_page_id: PageId
}

/// Database metadata
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseSchema {
    pub tables: HashMap<String, TableSchema>,
}

#[derive(Serialize, Deserialize)]
pub struct CatalogData {
    pub databases: HashMap<String, DatabaseSchema>,
}

pub struct Catalog {
    dbms_root_dir: PathBuf,
    data: CatalogData,
}

impl Catalog {
    pub fn new(dbms_root_dir: &Path) -> Self {
        // load catalog data
        let catalog_path = dbms_root_dir.join(CATALOG_FILE);

        let data = match Self::load_catalog(&catalog_path) {
            Ok(data) => data,
            Err(_) => {
                // create new catalog file
                let _ = Self::create_empty_catalog(&catalog_path);
                CatalogData { databases: HashMap::new() }
            }
        };

        Catalog {
            dbms_root_dir: dbms_root_dir.to_path_buf(),
            data,
        }
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.dbms_root_dir.join(CATALOG_FILE)
    }

    fn load_catalog(path: &Path) -> io::Result<CatalogData> {
        let text = std::fs::read_to_string(path)?;
        let data = serde_json::from_str::<CatalogData>(&text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(data)
    }

    pub fn save_catalog(&self) -> io::Result<()> {
        let json = serde_json::to_string_pretty(&self.data)?;
        fs::write(self.catalog_path(), json)
    }

    fn create_empty_catalog(path: &Path) -> io::Result<()> {
        let empty = CatalogData { databases: HashMap::new() };
        let json = serde_json::to_string_pretty(&empty)?;
        fs::write(path, json)?;
        Ok(())
    }

    pub fn has_database(&self, name: &str) -> bool {
        self.data.databases.contains_key(name)
    }

    pub fn add_database(&mut self, name: String) {
        let name = name.to_uppercase();
        self.data.databases.entry(name).or_insert_with(DatabaseSchema::default);
        self.save_catalog().expect("Failure to update catalog during CREATE DATABASE");
    }

    pub fn remove_database(&mut self, name: &str) {
        let name = &name.to_uppercase();
        self.data.databases.remove(name);
        self.save_catalog().expect("Failed to update catalog during DROP DATABASE");
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.data.databases.keys().cloned().collect()
    }

    pub fn has_table(&self, db: &str, table: &str) -> bool {
        let db = &db.to_uppercase();
        let table = &table.to_uppercase();
        self.data.databases
            .get(db)
            .map(|schema| schema.tables.contains_key(table))
            .unwrap_or(false)
    }

    pub fn add_table(&mut self, db: &str, table_schema: TableSchema) -> Result<(), String> {
        let db = &db.to_uppercase();
        let database = self.data.databases
            .get_mut(db)
            .ok_or_else(|| format!("Database '{}' does not exist", db))?;

        if database.tables.contains_key(table_schema.name.as_str()) {
            return Err(format!("Table '{}' already exists", table_schema.name));
        }

        database.tables.insert(table_schema.name.clone(), table_schema);
        self.save_catalog().map_err(|e| format!("Failed to add table: {}", e))?;
        Ok(())
    }

    pub fn remove_table(&mut self, db: &str, table: &str) -> Result<(), String> {
        let db = &db.to_uppercase();
        let table = &table.to_uppercase();
        let database = self.data.databases
            .get_mut(db)
            .ok_or_else(|| format!("Database '{}' does not exist", db))?;

        database.tables.remove(table);
        self.save_catalog().map_err(|e| format!("Failed to remove table: {}", e))?;
        Ok(())
    }

    pub fn get_table_schema(&self, db: &str, table: &str) -> Option<&TableSchema> {
        let db = &db.to_uppercase();
        let table = &table.to_uppercase();
        self.data.databases.get(db)?.tables.get(table)
    }
}