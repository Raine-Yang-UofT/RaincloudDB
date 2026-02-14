use crate::compiler::ast::{Literal, RowDef};
use crate::compiler::bounded_ast::BoundExpr;
use crate::interpreter::executor::{Executor, ExprContext};
use crate::types::ColumnId;

impl Executor {
    pub fn execute_expression(&self, expr: &BoundExpr, ctx: &ExprContext) -> Result<Literal, String> {
        match expr {
            BoundExpr::Literal(_, literal) => Ok(literal.clone()),
            BoundExpr::Column(_, index) =>
                self.execute_column(*index, &ctx.row),
            BoundExpr::Equals(_, lfs, rhs) =>
                self.execute_equal(lfs, rhs, &ctx)
        }
    }

    pub fn execute_column(&self, index: ColumnId, row: &RowDef) -> Result<Literal, String> {
        row.record.get(index)
            .cloned()
            .ok_or_else(|| format!("column {} not found", index))
    }

    pub fn execute_equal(&self, lfs: &BoundExpr, rhs: &BoundExpr, ctx: &ExprContext) -> Result<Literal, String> {
        let lfs = self.execute_expression(lfs, ctx)?;
        let rhs = self.execute_expression(rhs, ctx)?;
        if lfs == rhs {
            return Ok(Literal::Bool(true));
        }
        Ok(Literal::Bool(false))
    }
}
