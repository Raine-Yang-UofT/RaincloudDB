mod database_analyzer;
mod table_ddl_analyzer;
mod expression_analyzer;
mod select_analyzer;

use std::sync::{Arc, RwLock};
use crate::compiler::ast::Statement;
use crate::compiler::bounded_ast::BoundStmt;
use crate::interpreter::ExecResult;
use crate::interpreter::execution_context::ExecutionContext;
use crate::types::DbResult;

pub struct Analyzer {
    context: Arc<RwLock<ExecutionContext>>,
}

impl Analyzer {
    pub fn new(context: Arc<RwLock<ExecutionContext>>) -> Self {
        Self { context }
    }

    pub fn analyze(&mut self, stmt: Statement) -> DbResult<BoundStmt> {
        match &stmt {
            Statement::CreateDatabase { name } => {
                self.analyze_create_database(name)
            }
            Statement::DropDatabase { name } => {
                self.analyze_drop_database(name)
            }
            Statement::ConnectDatabase { name } => {
                self.analyze_connect_database(name)
            }
            Statement::DisconnectDatabase {} => {
                self.analyze_disconnect_database()
            }
            Statement::CreateTable { name, columns } => {
                self.analyze_create_table(name, columns)
            }
            Statement::DropTable { name } => {
                self.analyze_drop_table(name)
            }
            Statement::Insert { table, rows } => {
                self.analyze_insert_table(table, rows)
            }
            Statement::Update { table, assignments, selection } => {
                self.analyze_update_table(table, assignments, selection)
            }
            Statement::Select { columns, table, selection } => {
                self.analyze_select(table, columns, selection)
            }
        }
    }
}
