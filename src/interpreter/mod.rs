pub mod execution_context;
pub mod catalog;
pub mod executor;

use execution_context::ExecutionContext;
use catalog::Catalog;
use crate::compiler::ast::Statement;
use crate::interpreter::executor::Executor;

pub struct Interpreter {
    context: ExecutionContext,
}

impl Interpreter {
    pub fn new(data_dir: &str) -> Self {
        let catalog = Catalog::new(data_dir);
        let context = ExecutionContext::new(data_dir, catalog);
        Self { context }
    }

    /// Entry point for SQL interpreter
    pub fn execute(&mut self, stmt: Statement) -> Result<String, String> {
        let mut executor = Executor::new(&mut self.context);
        executor.execute(stmt)
    }
}