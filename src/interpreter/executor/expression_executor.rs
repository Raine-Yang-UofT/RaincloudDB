use crate::compiler::ast::{Literal, RowDef};
use crate::compiler::bounded_ast::BoundExpr;
use crate::interpreter::executor::{Executor, ExprContext};
use crate::types::{ColumnId, DbError, DbResult};

impl Executor {
    pub fn execute_expression(&self, expr: &BoundExpr, ctx: &ExprContext) -> DbResult<Literal> {
        match expr {
            BoundExpr::Literal(_, literal) => Ok(literal.clone()),
            BoundExpr::Column(_, index) =>
                self.execute_column(*index, &ctx.row),
            BoundExpr::Equals(_, lfs, rhs) =>
                self.execute_equal(lfs, rhs, &ctx)
        }
    }

    pub fn execute_column(&self, index: ColumnId, row: &RowDef) -> DbResult<Literal> {
        row.record.get(index)
            .cloned()
            .ok_or_else(|| DbError::ColumnNotFound(format!("column {} not found", index)))
    }

    pub fn execute_equal(&self, lfs: &BoundExpr, rhs: &BoundExpr, ctx: &ExprContext) -> DbResult<Literal> {
        let lfs = self.execute_expression(lfs, ctx)?;
        let rhs = self.execute_expression(rhs, ctx)?;
        if lfs == rhs {
            return Ok(Literal::Bool(true));
        }
        Ok(Literal::Bool(false))
    }
}
