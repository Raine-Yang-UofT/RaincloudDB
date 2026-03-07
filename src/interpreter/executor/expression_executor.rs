use crate::compiler::ast::{Literal, RowDef};
use crate::compiler::bounded_ast::BoundExpr;
use crate::interpreter::executor::{Executor, ExprContext};
use crate::types::{ColumnId, DbError, DbResult};

impl Executor {
    pub fn execute_expression(&self, expr: &BoundExpr, ctx: &ExprContext) -> DbResult<Literal> {
        match expr {
            // primary
            BoundExpr::Literal(lit) =>
                Ok(lit.clone()),
            BoundExpr::Column(index) =>
                self.execute_column(*index, &ctx.row),

            // comparisons
            BoundExpr::Equals(l, r) =>
                self.eval_cmp(l, r, ctx, |a, b| a == b),
            BoundExpr::NotEquals(l, r) =>
                self.eval_cmp(l, r, ctx, |a, b| a != b),
            BoundExpr::Gt(l, r) =>
                self.eval_cmp(l, r, ctx, |a, b| a > b),
            BoundExpr::Gte(l, r) =>
                self.eval_cmp(l, r, ctx, |a, b| a >= b),
            BoundExpr::Lt(l, r) =>
                self.eval_cmp(l, r, ctx, |a, b| a < b),
            BoundExpr::Lte(l, r) =>
                self.eval_cmp(l, r, ctx, |a, b| a <= b),

            // logical
            BoundExpr::And(l, r) =>
                self.eval_and(l, r, ctx),
            BoundExpr::Or(l, r) =>
                self.eval_or(l, r, ctx),

            // arithmetic
            BoundExpr::Add(l, r) =>
                self.eval_arith(l, r, ctx, |a, b| a + b),
            BoundExpr::Sub(l, r) =>
                self.eval_arith(l, r, ctx, |a, b| a - b),
            BoundExpr::Mul(l, r) =>
                self.eval_arith(l, r, ctx, |a, b| a * b),
            BoundExpr::Div(l, r) =>
                self.eval_arith(l, r, ctx, |a, b| a / b),

            // unary
            BoundExpr::Minus(e) =>
                self.eval_minus(e, ctx),
            BoundExpr::Not(e) =>
                self.eval_not(e, ctx),
        }
    }

    // column helper
    pub fn execute_column(&self, index: ColumnId, row: &RowDef) -> DbResult<Literal> {
        row.record.get(index)
            .cloned()
            .ok_or_else(|| DbError::ColumnNotFound(format!("column {} not found", index)))
    }

    // comparison helper
    fn eval_cmp<F>(
        &self, lhs: &BoundExpr, rhs: &BoundExpr, ctx: &ExprContext, cmp_func: F) -> DbResult<Literal>
    where
        F: Fn(&Literal, &Literal) -> bool,
    {
        let lhs = self.execute_expression(lhs, ctx)?;
        let rhs = self.execute_expression(rhs, ctx)?;
        Ok(Literal::Bool(cmp_func(&lhs, &rhs)))
    }

    // arithmetic helper
    fn eval_arith<F>(&self, lhs: &BoundExpr, rhs: &BoundExpr, ctx: &ExprContext, func: F) -> DbResult<Literal>
    where
        F: Fn(i32, i32) -> i32,
    {
        let lhs = self.execute_expression(lhs, ctx)?;
        let rhs = self.execute_expression(rhs, ctx)?;

        match (lhs, rhs) {
            (Literal::Int(a), Literal::Int(b)) =>
                Ok(Literal::Int(func(a, b))),
            _ =>
                Err(DbError::TypeMismatch("Arithmetic requires numerical operands".to_string())),
        }
    }

    // logical AND helper
    fn eval_and(&self, lhs: &BoundExpr, rhs: &BoundExpr, ctx: &ExprContext) -> DbResult<Literal> {
        let lhs = self.execute_expression(lhs, ctx)?;
        if let Literal::Bool(false) = lhs {
            return Ok(Literal::Bool(false)); // short circuit
        }

        let rhs = self.execute_expression(rhs, ctx)?;
        match rhs {
            Literal::Bool(b) => Ok(Literal::Bool(b)),
            _ => Err(DbError::TypeMismatch("AND requires BOOL operands".to_string()))
        }
    }

    // logical OR helper
    fn eval_or(&self, lhs: &BoundExpr, rhs: &BoundExpr, ctx: &ExprContext, ) -> DbResult<Literal> {
        let lhs = self.execute_expression(lhs, ctx)?;
        if let Literal::Bool(true) = lhs {
            return Ok(Literal::Bool(true)); // short circuit
        }

        let rhs = self.execute_expression(rhs, ctx)?;
        match rhs {
            Literal::Bool(b) => Ok(Literal::Bool(b)),
            _ => Err(DbError::TypeMismatch("OR requires BOOL operands".to_string()))
        }
    }

    // NOT helper
    fn eval_not(&self, expr: &BoundExpr, ctx: &ExprContext) -> DbResult<Literal> {
        match self.execute_expression(expr, ctx)? {
            Literal::Bool(v) => Ok(Literal::Bool(!v)),
            _ => Err(DbError::TypeMismatch("NOT requires BOOL".to_string()))
        }
    }

    // minus sign helper
    fn eval_minus(&self, expr: &BoundExpr, ctx: &ExprContext) -> DbResult<Literal> {

        match self.execute_expression(expr, ctx)? {
            Literal::Int(v) => Ok(Literal::Int(-v)),
            _ => Err(DbError::TypeMismatch("Unary minus requires numerical type".to_string()))
        }
    }
}
