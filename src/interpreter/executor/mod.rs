mod database_executor;
mod table_ddl_executor;

use std::sync::{Arc, RwLock};
use crate::compiler::ast::Statement;
use crate::interpreter::execution_context::ExecutionContext;

pub struct Executor {
    context: Arc<RwLock<ExecutionContext>>,
}

impl Executor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>) -> Self {
        Self { context }
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<String, String> {
        // only database-level statements are permitted if a database connection does not exist
        if self.context.read().unwrap().current_db.is_none() {
            if !matches!(stmt,
                Statement::CreateDatabase { name: _ } |
                Statement::DropDatabase { name: _ } |
                Statement::ConnectDatabase { name: _ } |
                Statement::DisconnectDatabase {}
            ) {
                return Err("A database connection does not exist".to_string());
            }
        }

        match stmt {
            Statement::CreateDatabase { name } => self.create_database(&name),
            Statement::DropDatabase { name } => self.drop_database(&name),
            Statement::ConnectDatabase { name } => self.connect_database(&name),
            Statement::DisconnectDatabase {} => self.disconnect_database(),
            Statement::CreateTable {name, columns } => self.create_table(&name , columns),
            _ => Err("Unsupported statement".to_string()),
        }
    }
}
