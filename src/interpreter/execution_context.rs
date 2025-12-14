use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use crate::interpreter::catalog::Catalog;
use crate::storage::{StorageConfig, StorageEngine};
use crate::types::{DEFAULT_BUFFERPOOL_REPLACEMENT, DEFAULT_BUFFERPOOL_SIZE};

pub struct ExecutionContext {
    pub dbms_root_dir: PathBuf,
    pub current_db: Option<String>,
    pub catalog: Catalog,
    pub storage_engines: HashMap<String, Arc<StorageEngine>>,
}

impl ExecutionContext {
    pub fn new(dbms_root_dir: PathBuf,
               catalog: Catalog) -> ExecutionContext {
        Self {
            dbms_root_dir,
            current_db: None,
            catalog,
            storage_engines: HashMap::new(),
        }
    }
    
    pub fn database_dir(&self, db: &str) -> PathBuf {
        self.dbms_root_dir.join(db)
    }

    pub fn get_storage_engine(&self) -> Result<Arc<StorageEngine>, String> {
        match self.current_db.clone() {
            Some(db) => match self.storage_engines.get(&db) {
                Some(engine) => Ok(engine.clone()),
                None => Err(String::from("The database does not exist.")),
            },
            None => Err(String::from("The database does not exist.")),
        }
    }
    
    pub fn initialize_storage_engine(&mut self, db_name: String) -> Result<(), String> {
        if self.storage_engines.contains_key(&db_name) {
            return Err(String::from("A storage engine for the database already exists"));
        }

        let storage_config = StorageConfig {
            database_dir: self.database_dir(&db_name),
            bufferpool_capacity: DEFAULT_BUFFERPOOL_SIZE,
            bufferpool_replacement_strategy: DEFAULT_BUFFERPOOL_REPLACEMENT
        };
        let storage_engine = StorageEngine::new(storage_config)
            .expect("Failed to create storage engine");
        self.storage_engines.insert(db_name, Arc::new(storage_engine));
        Ok(())
    }
}