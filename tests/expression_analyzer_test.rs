mod common;

use std::collections::HashMap;
use raincloud_db::compiler::ast::{
    ColumnDef, DataType, ExprType, Expression, Literal,
    BinaryOp, UnaryOp,
};
use raincloud_db::compiler::bounded_ast::{BoundExpr, BoundExprNode};
use raincloud_db::interpreter::analyzer::Analyzer;
use raincloud_db::interpreter::catalog::TableSchema;
use raincloud_db::types::ColumnId;
use crate::common::setup_interpreter;

fn setup_analyzer() -> Analyzer {
    let interpreter = setup_interpreter();
    Analyzer::new(interpreter.context)
}

fn test_schema() -> TableSchema {
    TableSchema {
        name: "".to_string(),
        columns: vec![
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Char(50),
            },
        ],
        column_index: HashMap::from([
            ("age".to_string(), 0),
            ("name".to_string(), 1),
        ]),
        first_page_id: 0,
    }
}

#[test]
fn test_literal_int_type() {
    let analyzer = setup_analyzer();
    let schema = test_schema();
    let expr = Expression::Literal(Literal::Int(10));
    let bound = analyzer.analyze_expression(&expr, &schema).unwrap();
    assert_eq!(
        bound,
        BoundExprNode {
            expr_type: ExprType::Int,
            expr: BoundExpr::Literal(Literal::Int(10)),
        }
    );
}

#[test]
fn test_literal_string_type() {
    let analyzer = setup_analyzer();
    let schema = test_schema();
    let expr = Expression::Literal(Literal::String("abc".to_string()));
    let bound = analyzer.analyze_expression(&expr, &schema).unwrap();
    assert_eq!(
        bound,
        BoundExprNode {
            expr_type: ExprType::Char,
            expr: BoundExpr::Literal(Literal::String("abc".to_string())),
        }
    );
}

#[test]
fn test_identifier_type() {
    let analyzer = setup_analyzer();
    let schema = test_schema();
    let expr = Expression::Identifier("name".to_string());
    let bound = analyzer.analyze_expression(&expr, &schema).unwrap();
    assert_eq!(
        bound,
        BoundExprNode {
            expr_type: ExprType::Char,
            expr: BoundExpr::Column(1 as ColumnId),
        }
    );
}

#[test]
fn test_equals() {
    let analyzer = setup_analyzer();
    let schema = test_schema();
    let expr = Expression::Binary {
        lhs: Box::new(Expression::Identifier("age".to_string())),
        op: BinaryOp::Eq,
        rhs: Box::new(Expression::Literal(Literal::Int(42))),
    };
    let bound = analyzer.analyze_expression(&expr, &schema).unwrap();
    assert_eq!(
        bound,
        BoundExprNode {
            expr_type: ExprType::Bool,
            expr: BoundExpr::Equals(
                Box::new(BoundExpr::Column(0 as ColumnId)),
                Box::new(BoundExpr::Literal(Literal::Int(42))),
            ),
        }
    );
}

#[test]
fn test_equals_type_mismatch() {
    let analyzer = setup_analyzer();
    let schema = test_schema();
    let expr = Expression::Binary {
        lhs: Box::new(Expression::Identifier("age".to_string())),
        op: BinaryOp::Eq,
        rhs: Box::new(Expression::Literal(Literal::String("bob".to_string()))),
    };
    assert!(analyzer.analyze_expression(&expr, &schema).is_err());
}

#[test]
fn test_unknown_identifier() {
    let analyzer = setup_analyzer();
    let schema = test_schema();
    let expr = Expression::Identifier("height".to_string());
    assert!(analyzer.analyze_expression(&expr, &schema).is_err());
}