mod database_executor;
mod table_ddl_executor;

use std::sync::{Arc, RwLock};
use crate::compiler::ast::Statement;
use crate::interpreter::execution_context::ExecutionContext;

pub struct Executor {
    pub context: Arc<RwLock<ExecutionContext>>,
}

impl Executor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>) -> Self {
        Self { context }
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<String, String> {
        match stmt {
            Statement::CreateDatabase { name } => self.create_database(&name),
            Statement::DropDatabase { name } => self.drop_database(&name),
            Statement::ConnectDatabase { name } => self.connect_database(&name),
            Statement::DisconnectDatabase {} => self.disconnect_database(),
            Statement::CreateTable {name, columns } => self.create_table(&name , columns),
            Statement::DropTable { name } => self.drop_table(&name),
            Statement::Insert { table, rows } => self.insert_table(&table, rows),
            _ => Err("Unsupported statement".to_string()),
        }
    }
}
