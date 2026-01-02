use crate::interpreter::analyzer::Analyzer;

impl Analyzer {

    pub fn analyze_create_database(&self, name: &str) -> Result<(), String> {
        let ctx = self.context.read().unwrap();

        // check no duplicate database name in catalog
        if ctx.catalog.has_database(name) {
            return Err(format!("Database {} already exists", name));
        }

        // check no duplicate database name in file system
        let database_dir = ctx.dbms_root_dir.join(name);
        if database_dir.exists() {
            return Err(format!("The database directory '{}' already exists.", name));
        }

        Ok(())
    }

    pub fn analyze_drop_database(&self, name: &str) -> Result<(), String> {
        let ctx = self.context.read().unwrap();

        // check the database exists in catalog
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

        Ok(())
    }

    pub fn analyze_connect_database(&self, name: &str) -> Result<(), String> {
        let ctx = self.context.read().unwrap();

        // check a connection does no already exist
        if ctx.current_db.is_some() {
            return Err("A database connection already exists".to_string());
        }

        // check the database exists
        if !ctx.catalog.has_database(name) {
            return Err(format!("Database '{}' does not exist", name));
        }

        Ok(())
    }
}