use std::cmp::PartialEq;
use crate::compiler::ast::{DataType, ExprType, Expression, Literal};
use crate::interpreter::analyzer::Analyzer;
use crate::interpreter::catalog::TableSchema;

impl Analyzer {
    pub fn analyze_expression(&self, expr: &Expression, schema: &TableSchema) -> Result<ExprType, String> {
        match expr {
            Expression::Literal(_) => self.analyze_literal(&expr),
            Expression::Identifier(_) => self.analyze_identifier(expr, &schema),
            Expression::Equals(l, r) => {
                let lfs = self.analyze_expression(l, schema)?;
                let rhs = self.analyze_expression(r, schema)?;
                if lfs != rhs {
                    return Err(format!("Mismatched type in '=' expression, LHS '{:?}' RHS '{:?}'", lfs, rhs));
                }
                Ok(ExprType::Bool)
            }
        }
    }

    fn analyze_literal(&self, expr: &Expression) -> Result<ExprType, String> {
        if let Expression::Literal(lit) = expr {
            return match lit {
                Literal::Int(_) => Ok(ExprType::Int),
                Literal::String(_) => Ok(ExprType::Char),
            }
        }
        Err("Expected literal".to_string())
    }

    fn analyze_identifier(&self, expr: &Expression, schema: &TableSchema) -> Result<ExprType, String> {
        if let Expression::Identifier(name) = expr {
            return schema.columns
                .iter()
                .find(|c| c.name == *name)
                .map(|c| self.parse_data_type(&c.data_type) )
                .ok_or_else(|| format!("Unknown column '{}'", name))
        }
        Err("Expected identifier expression".to_string())
    }

    fn parse_data_type(&self, data_type: &DataType) -> ExprType {
        match data_type {
            DataType::Int => ExprType::Int,
            DataType::Char(_) => ExprType::Char,
        }
    }
}
