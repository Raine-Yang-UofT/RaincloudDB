use std::path::Path;
use crate::interpreter::executor::Executor;
use crate::types::{DATA_FILE, HEADER_FILE};

impl Executor {
    pub fn create_database(&mut self, name: &str) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();

        // the database already exists in catalog
        if ctx.catalog.has_database(name) {
            return Err(format!("Database {} already exists", name));
        }

        // the database directory exists in file system
        let database_dir = ctx.dbms_root_dir.join(name);
        if database_dir.exists() {
            return Err(format!("The database directory '{}' already exists.", name));
        }

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

        // check the database exists
        if !ctx.catalog.has_database(name) {
            return Err(format!("Database '{}' does not exist", name));
        }

        // check the database is not in use
        if ctx.current_db.as_deref() == Some(name) {
            return Err(format!(
                "Cannot drop the currently selected database '{}'",
                name
            ));
        }

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

        // check a connection already exists
        if ctx.current_db.is_some() {
            return Err("A database connection already exists".to_string());
        }

        // check the database exists
        if !ctx.catalog.has_database(name) {
            return Err(format!("Database '{}' does not exist", name));
        }

        ctx.current_db = Some(name.to_string());
        Ok(format!("Connected to {}", name))
    }

    pub fn disconnect_database(&mut self) -> Result<String, String> {
        let mut ctx = self.context.write().unwrap();
        ctx.current_db = None;
        Ok("Disconnected from database".to_string())
    }
}