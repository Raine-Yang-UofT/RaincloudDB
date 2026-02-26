use std::path::Path;
use crate::interpreter::ExecResult;
use crate::interpreter::executor::Executor;
use crate::types::{DbError, DbResult, DATA_FILE, HEADER_FILE};

impl Executor {
    pub fn create_database(&mut self, name: &str) -> DbResult<ExecResult> {
        let mut ctx = self.context.write().unwrap();
        let database_dir = ctx.dbms_root_dir.join(name);

        // create data files
        std::fs::create_dir_all(&database_dir).map_err(|e| DbError::InternalError(e.to_string()))?;
        std::fs::File::create(database_dir.join(DATA_FILE)).map_err(|e| DbError::InternalError(e.to_string()))?;
        std::fs::File::create(database_dir.join(HEADER_FILE)).map_err(|e| DbError::InternalError(e.to_string()))?;

        // add database to catalog
        ctx.catalog.add_database(name.to_string());

        // initialize storage engine
        ctx.initialize_storage_engine(String::from(name))
            .expect("Failed to register storage engine");

        Ok(ExecResult::Success(format!("Database '{}' created successfully", name)))
    }

    pub fn drop_database(&mut self, name: &str) -> DbResult<ExecResult> {
        let mut ctx = self.context.write().unwrap();

        // remove database directory
        let root = Path::new(&ctx.dbms_root_dir);
        let db_path = root.join(name);
        if db_path.exists() {
            std::fs::remove_dir_all(&db_path)
                .map_err(|e| DbError::InternalError(format!("Failed to delete database '{}': {}", name, e)))?;
        }

        // remove database from catalog
        ctx.catalog.remove_database(name);
        ctx.storage_engines.remove(name);

        Ok(ExecResult::Success(format!("Database '{}' dropped successfully.", name)))
    }

    pub fn connect_database(&mut self, name: &str) -> DbResult<ExecResult> {
        let mut ctx = self.context.write().unwrap();
        ctx.current_db = Some(name.to_string());
        Ok(ExecResult::Success(format!("Connected to {}", name)))
    }

    pub fn disconnect_database(&mut self) -> DbResult<ExecResult> {
        let mut ctx = self.context.write().unwrap();
        ctx.current_db = None;
        Ok(ExecResult::Success("Disconnected from database".to_string()))
    }
}