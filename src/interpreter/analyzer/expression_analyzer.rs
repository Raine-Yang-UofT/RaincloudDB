use crate::compiler::ast::{DataType, Literal, Expression, ExprType};
use crate::interpreter::analyzer::Analyzer;
use crate::interpreter::catalog::TableSchema;

impl Analyzer {
    pub fn analyze_expression(&self, expr: &Expression, schema: &TableSchema) -> Result<ExprType, String> {
        match expr {
            Expression::Literal(lit) => self.analyze_literal(lit),
            Expression::Identifier(name) => self.analyze_identifier(name, &schema),
            Expression::Equals(lhs, rhs) => self.analyze_equal(lhs, rhs, &schema),
        }
    }

    fn analyze_literal(&self, lit: &Literal) -> Result<ExprType, String> {
        match lit {
            Literal::Int(_) => Ok(ExprType::Int),
            Literal::String(_) => Ok(ExprType::Char),
            Literal::Bool(_) => Ok(ExprType::Bool),
        }
    }

    fn analyze_identifier(&self, name: &String, schema: &TableSchema) -> Result<ExprType, String> {
        schema.columns
            .iter()
            .find(|c| c.name == *name)
            .map(|c| self.parse_data_type(&c.data_type) )
            .ok_or_else(|| format!("Unknown column '{}'", name))
    }

    fn analyze_equal(&self, lhs: &Expression, rhs: &Expression, schema: &TableSchema) -> Result<ExprType, String> {
        let left = self.analyze_expression(lhs, schema)?;
        let right = self.analyze_expression(rhs, schema)?;
        if left != right {
            return Err(format!("Mismatched type in '=' expression, LHS '{:?}' RHS '{:?}'", left, right));
        }
        Ok(ExprType::Bool)
    }

    fn parse_data_type(&self, data_type: &DataType) -> ExprType {
        match data_type {
            DataType::Int => ExprType::Int,
            DataType::Char(_) => ExprType::Char,
        }
    }
}
