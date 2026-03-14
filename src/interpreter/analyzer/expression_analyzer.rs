use crate::compiler::ast::{DataType, Literal, Expression, ExprType, UnaryOp, BinaryOp};
use crate::compiler::bounded_ast::{BoundExpr, BoundExprNode};
use crate::interpreter::analyzer::Analyzer;
use crate::interpreter::catalog::TableSchema;
use crate::types::{DbError, DbResult};

impl Analyzer {

    /// analyze WHERE condition in SQL statement
    pub fn analyze_where_clause(&self, selection: &Expression, schema: &TableSchema) -> DbResult<BoundExprNode> {
        let bound = self.analyze_expression(selection, schema)?;
        if bound.expr_type != ExprType::Bool {
            return Err(DbError::TypeMismatch("WHERE clause must evaluate to a boolean expression".to_string()));
        }
        Ok(bound)
    }

    pub fn analyze_expression(&self, expr: &Expression, schema: &TableSchema) -> DbResult<BoundExprNode> {
        match expr {
            Expression::Literal(lit) => self.analyze_literal(lit),
            Expression::Identifier(name) =>
                self.analyze_identifier(name, &schema),
            Expression::Unary { op, expr } =>
                self.analyze_unary(op, expr, &schema),
            Expression::Binary { op, lhs, rhs } => 
                self.analyze_binary(op, lhs, rhs, schema),
        }
    }

    fn analyze_literal(&self, lit: &Literal) -> DbResult<BoundExprNode> {
        match lit {
            Literal::Int(_) => Ok(BoundExprNode{ expr_type: ExprType::Int, expr: BoundExpr::Literal(lit.clone())}),
            Literal::String(_) => Ok(BoundExprNode{ expr_type: ExprType::Char, expr: BoundExpr::Literal(lit.clone())}),
            Literal::Bool(_) => Ok(BoundExprNode{ expr_type: ExprType::Bool, expr: BoundExpr::Literal(lit.clone())}),
        }
    }

    fn analyze_identifier(&self, name: &String, schema: &TableSchema) -> DbResult<BoundExprNode> {
        let column_id = *schema.column_index.get(name)
            .ok_or_else(|| DbError::ColumnNotFound(format!("Unknown column '{}'", name)))?;
        let column_def = &schema.columns[column_id];
        let expr_type = self.parse_data_type(&column_def.data_type);
        Ok(BoundExprNode{ expr_type, expr: BoundExpr::Column(column_id) })
    }

    fn analyze_binary(
        &self,
        op: &BinaryOp,
        lhs: &Expression,
        rhs: &Expression,
        schema: &TableSchema,
    ) -> DbResult<BoundExprNode> {
        let left = self.analyze_expression(lhs, schema)?;
        let right = self.analyze_expression(rhs, schema)?;
        match op {
            // comparison operators
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Gt
            | BinaryOp::Gte
            | BinaryOp::Lt
            | BinaryOp::Lte => {
                if left.expr_type != right.expr_type {
                    return Err(DbError::TypeMismatch(format!(
                        "Mismatched type, LHS '{:?}' RHS '{:?}'",
                        left.expr_type, right.expr_type
                    )));
                }
                let expr = match op {
                    BinaryOp::Eq =>
                        BoundExpr::Equals(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::NotEq =>
                        BoundExpr::NotEquals(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Gt =>
                        BoundExpr::Gt(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Gte =>
                        BoundExpr::Gte(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Lt =>
                        BoundExpr::Lt(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Lte =>
                        BoundExpr::Lte(Box::new(left.expr), Box::new(right.expr)),
                    _ => unreachable!(),
                };
                Ok(BoundExprNode {
                    expr_type: ExprType::Bool,
                    expr,
                })
            }
            // logical operators
            BinaryOp::And | BinaryOp::Or => {
                if left.expr_type != ExprType::Bool || right.expr_type != ExprType::Bool {
                    return Err(DbError::TypeMismatch(
                        "Logical operators require boolean operands".to_string()
                    ));
                }
                let expr = match op {
                    BinaryOp::And =>
                        BoundExpr::And(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Or =>
                        BoundExpr::Or(Box::new(left.expr), Box::new(right.expr)),
                    _ => unreachable!(),
                };
                Ok(BoundExprNode {
                    expr_type: ExprType::Bool,
                    expr,
                })
            }
            // arithmetic operators
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div => {
                if left.expr_type != right.expr_type {
                    return Err(DbError::TypeMismatch(format!(
                        "Arithmetic type mismatch LHS '{:?}' RHS '{:?}'",
                        left.expr_type, right.expr_type
                    )));
                }
                let expr = match op {
                    BinaryOp::Add =>
                        BoundExpr::Add(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Sub =>
                        BoundExpr::Sub(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Mul =>
                        BoundExpr::Mul(Box::new(left.expr), Box::new(right.expr)),
                    BinaryOp::Div =>
                        BoundExpr::Div(Box::new(left.expr), Box::new(right.expr)),
                    _ => unreachable!(),
                };
                Ok(BoundExprNode {
                    expr_type: left.expr_type,
                    expr,
                })
            }
        }
    }

    fn analyze_unary(&self, op: &UnaryOp, expr: &Expression, schema: &TableSchema) -> DbResult<BoundExprNode> {
        let node = self.analyze_expression(expr, schema)?;
        match op {
            UnaryOp::Neg => {
                // negative sign requires numerical type
                if node.expr_type == ExprType::Int {
                    Ok(BoundExprNode{ expr_type: node.expr_type, expr: BoundExpr::Minus(Box::new(node.expr))})
                } else {
                    Err(DbError::TypeMismatch(
                        format!("Expect numerical type after '-', got '{:?}' of type {:?}'", node.expr, node.expr_type)))
                }
            }
            UnaryOp::Not => {
                // NOT requires boolean type
                if node.expr_type == ExprType::Bool {
                    Ok(BoundExprNode{ expr_type: node.expr_type, expr: BoundExpr::Not(Box::new(node.expr))})
                } else {
                    Err(DbError::TypeMismatch(
                        format!("Expect bool type after 'NOT', got '{:?}'", expr)))
                }
            }
        }
    }

    fn parse_data_type(&self, data_type: &DataType) -> ExprType {
        match data_type {
            DataType::Int => ExprType::Int,
            DataType::Char(_) => ExprType::Char,
        }
    }
}
