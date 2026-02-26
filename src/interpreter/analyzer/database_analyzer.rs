use crate::compiler::bounded_ast::BoundStmt;
use crate::interpreter::analyzer::Analyzer;
use crate::types::{DbError, DbResult};

impl Analyzer {

    pub fn analyze_create_database(&self, name: &str) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check no duplicate database name in catalog
        if ctx.catalog.has_database(name) {
            return Err(DbError::DuplicateDatabase(format!("Database {} already exists", name)));
        }

        // check no duplicate database name in file system
        let database_dir = ctx.dbms_root_dir.join(name);
        if database_dir.exists() {
            return Err(DbError::DuplicateDatabase(format!("The database directory '{}' already exists.", name)));
        }

        Ok(BoundStmt::CreateDatabase { name: String::from(name) })
    }

    pub fn analyze_drop_database(&self, name: &str) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check the database exists in catalog
        if !ctx.catalog.has_database(name) {
            return Err(DbError::DatabaseNotFound(format!("Database '{}' does not exist", name)));
        }

        // check the database is not in use
        if ctx.current_db.as_deref() == Some(name) {
            return Err(DbError::ConnectionExist(format!(
                "Cannot drop the currently selected database '{}'",
                name
            )));
        }

        Ok(BoundStmt::DropDatabase { name: String::from(name) })
    }

    pub fn analyze_connect_database(&self, name: &str) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check a connection does no already exist
        if ctx.current_db.is_some() {
            return Err(DbError::ConnectionNotFound("A database connection already exists".to_string()));
        }

        // check the database exists
        if !ctx.catalog.has_database(name) {
            return Err(DbError::DuplicateDatabase(format!("Database {} already exists", name)));
        }

        Ok(BoundStmt::ConnectDatabase { name: String::from(name) })
    }

    pub fn analyze_disconnect_database(&self) -> DbResult<BoundStmt> {
        Ok(BoundStmt::DisconnectDatabase)
    }
}