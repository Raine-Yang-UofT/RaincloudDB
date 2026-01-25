mod common;

use raincloud_db::compiler::ast::{ColumnDef, DataType, ExprType, Expression, Literal};
use raincloud_db::interpreter::analyzer::Analyzer;
use raincloud_db::interpreter::catalog::TableSchema;
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
        first_page_id: 0,
    }
}

#[test]
fn test_literal_int_type() {
    let analyzer = setup_analyzer();
    let schema = test_schema();

    let expr = Expression::Literal(Literal::Int(10));
    let ty = analyzer.analyze_expression(&expr, &schema).unwrap();

    assert_eq!(ty, ExprType::Int);
}

#[test]
fn test_literal_string_type() {
    let analyzer = setup_analyzer();
    let schema = test_schema();

    let expr = Expression::Literal(Literal::String("abc".to_string()));
    let ty = analyzer.analyze_expression(&expr, &schema).unwrap();

    assert_eq!(ty, ExprType::Char);
}

#[test]
fn test_identifier_type() {
    let analyzer = setup_analyzer();
    let schema = test_schema();

    let expr = Expression::Identifier("name".to_string());
    let ty = analyzer.analyze_expression(&expr, &schema).unwrap();

    assert_eq!(ty, ExprType::Char);
}

#[test]
fn test_equals() {
    let analyzer = setup_analyzer();
    let schema = test_schema();

    let expr = Expression::Equals(
        Box::new(Expression::Identifier("age".to_string())),
        Box::new(Expression::Literal(Literal::Int(42))),
    );

    let ty = analyzer.analyze_expression(&expr, &schema).unwrap();
    assert_eq!(ty, ExprType::Bool);
}

#[test]
fn test_equals_type_mismatch() {
    let analyzer = setup_analyzer();
    let schema = test_schema();

    let expr = Expression::Equals(
        Box::new(Expression::Identifier("age".to_string())),
        Box::new(Expression::Literal(Literal::String("bob".to_string()))),
    );

    let err = analyzer.analyze_expression(&expr, &schema).unwrap_err();
    assert!(err.contains("Mismatched type"));
}


#[test]
fn test_unknown_identifier() {
    let analyzer = setup_analyzer();
    let schema = test_schema();

    let expr = Expression::Identifier("height".to_string());
    assert!(analyzer.analyze_expression(&expr, &schema).is_err());
}
