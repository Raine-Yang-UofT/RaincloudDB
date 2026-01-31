use crate::compiler::ast::{DataType, Literal, Expression};
use crate::interpreter::analyzer::Analyzer;
use crate::interpreter::catalog::TableSchema;

impl Analyzer {
    pub fn analyze_expression(&self, expr: &Expression, schema: &TableSchema) -> Result<Literal, String> {
        match expr {
            Expression::Literal(lit) => Ok(lit.clone()),
            Expression::Identifier(_) => self.analyze_identifier(expr, &schema),
            Expression::Equals(l, r) => {
                let lfs = self.analyze_expression(l, schema)?;
                let rhs = self.analyze_expression(r, schema)?;
                if lfs != rhs {
                    return Err(format!("Mismatched type in '=' expression, LHS '{:?}' RHS '{:?}'", lfs, rhs));
                }
                Ok(Literal::Bool(false))
            }
        }
    }

    fn analyze_identifier(&self, expr: &Expression, schema: &TableSchema) -> Result<Literal, String> {
        if let Expression::Identifier(name) = expr {
            return schema.columns
                .iter()
                .find(|c| c.name == *name)
                .map(|c| self.parse_data_type(&c.data_type) )
                .ok_or_else(|| format!("Unknown column '{}'", name))
        }
        Err("Expected identifier expression".to_string())
    }

    fn parse_data_type(&self, data_type: &DataType) -> Literal {
        match data_type {
            DataType::Int => Literal::Int(0),
            DataType::Char(_) => Literal::String("".to_string()),
        }
    }
}
