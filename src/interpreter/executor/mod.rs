mod database_executor;
mod table_ddl_executor;
mod expression_executor;

use std::sync::{Arc, RwLock};
use crate::compiler::ast::RowDef;
use crate::compiler::bounded_ast::BoundStmt;
use crate::interpreter::execution_context::ExecutionContext;

pub struct Executor {
    pub context: Arc<RwLock<ExecutionContext>>,
}

/// Context for executing an expression
pub struct ExprContext<'a> {
    pub row: &'a RowDef,
}


impl Executor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>) -> Self {
        Self { context }
    }

    pub fn execute(&mut self, stmt: BoundStmt) -> Result<String, String> {
        match stmt {
            BoundStmt::CreateDatabase { name } => self.create_database(&name),
            BoundStmt::DropDatabase { name } => self.drop_database(&name),
            BoundStmt::ConnectDatabase { name } => self.connect_database(&name),
            BoundStmt::DisconnectDatabase {} => self.disconnect_database(),
            BoundStmt::CreateTable {name, columns } => self.create_table(&name , columns),
            BoundStmt::DropTable { name } => self.drop_table(&name),
            BoundStmt::Insert { table, rows } => self.insert_table(&table, &rows),
            BoundStmt::Update { table, assignments, selection } => 
                self.update_table(&table, &assignments, &selection),
            _ => Err("Unsupported statement".to_string()),
        }
    }
}
