use crate::compiler::ast::{DataType, Literal, Expression, ExprType};
use crate::compiler::bounded_ast::BoundExpr;
use crate::interpreter::analyzer::Analyzer;
use crate::interpreter::catalog::TableSchema;

impl Analyzer {
    pub fn analyze_expression(&self, expr: &Expression, schema: &TableSchema) -> Result<BoundExpr, String> {
        match expr {
            Expression::Literal(lit) => self.analyze_literal(lit),
            Expression::Identifier(name) => self.analyze_identifier(name, &schema),
            Expression::Equals(lhs, rhs) => self.analyze_equal(lhs, rhs, &schema),
        }
    }

    fn analyze_literal(&self, lit: &Literal) -> Result<BoundExpr, String> {
        match lit {
            Literal::Int(_) => Ok(BoundExpr::Literal(ExprType::Int, lit.clone())),
            Literal::String(_) => Ok(BoundExpr::Literal(ExprType::Char, lit.clone())),
            Literal::Bool(_) => Ok(BoundExpr::Literal(ExprType::Bool, lit.clone())),
        }
    }

    fn analyze_identifier(&self, name: &String, schema: &TableSchema) -> Result<BoundExpr, String> {
        let column_id = *schema.column_index.get(name)
            .ok_or_else(|| format!("Unknown column '{}'", name))?;
        let column_def = &schema.columns[column_id];
        let expr_type = self.parse_data_type(&column_def.data_type);
        Ok(BoundExpr::Column (expr_type, column_id, ))
    }

    fn analyze_equal(&self, lhs: &Expression, rhs: &Expression, schema: &TableSchema) -> Result<BoundExpr, String> {
        let left = self.analyze_expression(lhs, schema)?;
        let right = self.analyze_expression(rhs, schema)?;
        if left.get_type() != right.get_type() {
            return Err(format!("Mismatched type in '=' expression, LHS '{:?}' RHS '{:?}'", left, right));
        }
        Ok(BoundExpr::Equals(ExprType::Bool, Box::new(left), Box::new(right)))
    }

    fn parse_data_type(&self, data_type: &DataType) -> ExprType {
        match data_type {
            DataType::Int => ExprType::Int,
            DataType::Char(_) => ExprType::Char,
        }
    }
}
