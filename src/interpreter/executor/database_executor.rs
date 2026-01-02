use std::path::Path;
use crate::interpreter::executor::Executor;
use crate::types::{DATA_FILE, HEADER_FILE};

impl Executor {
    pub fn create_database(&mut self, name: &str) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();
        let database_dir = ctx.dbms_root_dir.join(name);

        // create data files
        std::fs::create_dir_all(&database_dir).map_err(|e| e.to_string())?;
        std::fs::File::create(database_dir.join(DATA_FILE)).map_err(|e| e.to_string())?;
        std::fs::File::create(database_dir.join(HEADER_FILE)).map_err(|e| e.to_string())?;

        // add database to catalog
        ctx.catalog.add_database(name.to_string());

        // initialize storage engine
        ctx.initialize_storage_engine(String::from(name))
            .expect("Failed to register storage engine");

        Ok(format!("Database '{}' created successfully", name))
    }

    pub fn drop_database(&mut self, name: &str) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();

        // remove database directory
        let root = Path::new(&ctx.dbms_root_dir);
        let db_path = root.join(name);
        if db_path.exists() {
            std::fs::remove_dir_all(&db_path)
                .map_err(|e| format!("Failed to delete database '{}': {}", name, e))?;
        }

        // remove database from catalog
        ctx.catalog.remove_database(name);
        ctx.storage_engines.remove(name);

        Ok(format!("Database '{}' dropped successfully.", name))
    }

    pub fn connect_database(&mut self, name: &str) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();
        ctx.current_db = Some(name.to_string());
        Ok(format!("Connected to {}", name))
    }

    pub fn disconnect_database(&mut self) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();
        ctx.current_db = None;
        Ok("Disconnected from database".to_string())
    }
}