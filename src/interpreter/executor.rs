use crate::compiler::ast::Statement;
use crate::interpreter::execution_context::ExecutionContext;
use crate::storage::{StorageConfig, StorageEngine};

pub struct Executor<'a> {
    context: &'a mut ExecutionContext,
}

impl<'a> Executor<'a> {
    pub fn new(context: &'a mut ExecutionContext) -> Self {
        Self { context }
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<String, String> {
        match stmt {
            Statement::CreateDatabase { name } => self.create_database(&name),
            Statement::DropDatabase { name } => self.drop_database(&name),
            _ => Err("Unsupported statement".to_string()),
        }
    }

    fn create_database(&mut self, name: &str) -> Result<String, String> {
        // the database already exists in catalog
        if self.context.catalog.has_database(name) {
            return Err(format!("Database {} already exists", name));
        }

        let root = std::path::Path::new(&self.context.config_dir);
        let db_path = root.join(name);

        // the database directory exists in file system
        if db_path.exists() {
            return Err(format!("The database directory '{}' already exists.", name));
        }

        // create directory and copy over global default config
        std::fs::create_dir_all(&db_path).map_err(|e| e.to_string())?;
        let default_cfg_path = root.join("config.json");
        let mut config = StorageConfig::load_config(&default_cfg_path)
            .map_err(|e| e.to_string())?;
        config.db_dir = db_path.clone();
        let db_cfg_path = db_path.join("config.json");
        config.save_config(&db_cfg_path).map_err(|e| e.to_string())?;

        // create data files
        std::fs::File::create(db_path.join(&config.data_file_name)).map_err(|e| e.to_string())?;
        std::fs::File::create(db_path.join(&config.header_file_name)).map_err(|e| e.to_string())?;
        StorageEngine::new(config).map_err(|e| e.to_string())?;

        // add database to catalog
        self.context.catalog.add_database(name.to_string());
        Ok(format!("Database '{}' created successfully", name))
    }

    fn drop_database(&mut self, name: &str) -> Result<String, String> {
        let root = std::path::Path::new(&self.context.config_dir);
        let db_path = root.join(name);

        // check the database exists
        if !self.context.catalog.has_database(name) {
            return Err(format!("Database {} does not exist", name));
        }

        // check the database is not in use
        if self.context.get_current_db() == Some(name) {
            return Err(format!("Cannot drop the currently selected database '{}'", name));
        }

        // remove database directory
        if db_path.exists() {
            std::fs::remove_dir_all(&db_path).map_err(|e| {
                format!("Failed to delete database '{}': {}", name, e)
            })?;
        }

        // remove database from catalog
        self.context.catalog.remove_database(name);
        Ok(format!("Database '{}' dropped successfully.", name))
    }
}