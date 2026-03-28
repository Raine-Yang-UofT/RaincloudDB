use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use crate::interpreter::catalog_table::Catalog;
use crate::interpreter::database_catalog::GlobalCatalog;
use crate::storage::{StorageConfig, StorageEngine};
use crate::types::{CATALOG_FILE, DEFAULT_BUFFERPOOL_REPLACEMENT, DEFAULT_BUFFERPOOL_SIZE};

pub struct ExecutionContext {
    pub dbms_root_dir: PathBuf,
    pub current_db: Option<String>,
    pub global_catalog: GlobalCatalog,
    pub catalogs: HashMap<String, Catalog>,
    pub storage_engines: HashMap<String, Arc<StorageEngine>>,
}

impl ExecutionContext {
    pub fn new(dbms_root_dir: PathBuf,
               catalog: GlobalCatalog) -> ExecutionContext {
        Self {
            dbms_root_dir,
            current_db: None,
            global_catalog: catalog,
            catalogs: HashMap::new(),
            storage_engines: HashMap::new(),
        }
    }
    
    pub fn database_dir(&self, db: &str) -> PathBuf {
        self.dbms_root_dir.join(db)
    }
    
    pub fn initialize_database_ctx(&mut self, db_name: String) -> Result<(), String> {
        // initialize storage engine
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
        self.storage_engines.insert(db_name.clone(), Arc::new(storage_engine));

        // initialize catalog tables
        self.catalogs.insert(
            db_name.clone(),
            Catalog::new(self.database_dir(&db_name).join(CATALOG_FILE))
        );

        Ok(())
    }
}