mod common;

use std::collections::HashMap;
use raincloud_db::compiler::ast::{ColumnDef, DataType, ExprType, Literal, RowDef};
use raincloud_db::compiler::bounded_ast::BoundExpr;
use raincloud_db::interpreter::catalog::TableSchema;
use raincloud_db::interpreter::executor::{Executor, ExprContext};
use crate::common::setup_interpreter;

fn setup_executor() -> Executor {
    let interpreter = setup_interpreter();
    Executor::new(interpreter.context)
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
        column_index: HashMap::from([("age".to_string(), 0), ("name".to_string(), 1)]),
        first_page_id: 0,
    }
}

#[test]
fn test_literal() {
    let executor = setup_executor();
    let ctx = ExprContext { row: RowDef { record: vec![] } };

    let expr1 = BoundExpr::Literal(ExprType::Int, Literal::Int(10));
    let expr2 = BoundExpr::Literal(ExprType::Char, Literal::String("Hello World!".to_string()));
    let expr3 = BoundExpr::Literal(ExprType::Bool, Literal::Bool(true));
    let expr4 = BoundExpr::Literal(ExprType::Int, Literal::Int(-42));

    assert_eq!(executor.execute_expression(&expr1, &ctx), Ok(Literal::Int(10)));
    assert_eq!(executor.execute_expression(&expr2, &ctx), Ok(Literal::String("Hello World!".to_string())));
    assert_eq!(executor.execute_expression(&expr3, &ctx), Ok(Literal::Bool(true)));
    assert_eq!(executor.execute_expression(&expr4, &ctx), Ok(Literal::Int(-42)));
}

#[test]
fn test_column_access() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: RowDef {
            record: vec![
                Literal::Int(25),
                Literal::String("Alice".to_string())
            ]
        }
    };

    assert_eq!(
        executor.execute_expression(&BoundExpr::Column(ExprType::Int, 0), &ctx),
        Ok(Literal::Int(25))
    );
    assert_eq!(
        executor.execute_expression(&BoundExpr::Column(ExprType::Char, 1), &ctx),
        Ok(Literal::String("Alice".to_string()))
    );
}

#[test]
fn test_column_out_of_bounds() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: RowDef { record: vec![Literal::Int(10)] }
    };

    assert!(executor.execute_expression(&BoundExpr::Column(ExprType::Int, 5), &ctx).is_err());
}

#[test]
fn test_equals_literals_true() {
    let executor = setup_executor();
    let ctx = ExprContext { row: RowDef { record: vec![] } };

    let expr = BoundExpr::Equals(
        ExprType::Bool,
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(10))),
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(10))),
    );

    assert_eq!(
        executor.execute_expression(&expr, &ctx),
        Ok(Literal::Bool(true))
    );
}

#[test]
fn test_equals_literals_false() {
    let executor = setup_executor();
    let ctx = ExprContext { row: RowDef { record: vec![] } };

    let expr = BoundExpr::Equals(
        ExprType::Bool,
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(10))),
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(20))),
    );

    assert_eq!(
        executor.execute_expression(&expr, &ctx),
        Ok(Literal::Bool(false))
    );
}

#[test]
fn test_equals_column_literal() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: RowDef { record: vec![Literal::Int(30)] }
    };

    let expr = BoundExpr::Equals(
        ExprType::Bool,
        Box::new(BoundExpr::Column(ExprType::Int, 0)),
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(30))),
    );

    assert_eq!(
        executor.execute_expression(&expr, &ctx),
        Ok(Literal::Bool(true))
    );
}

#[test]
fn test_equals_column_column() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: RowDef {
            record: vec![Literal::Int(10), Literal::Int(10)]
        }
    };

    let expr = BoundExpr::Equals(
        ExprType::Bool,
        Box::new(BoundExpr::Column(ExprType::Int, 0)),
        Box::new(BoundExpr::Column(ExprType::Int, 1)),
    );

    assert_eq!(
        executor.execute_expression(&expr, &ctx),
        Ok(Literal::Bool(true))
    );
}

#[test]
fn test_nested_equals() {
    let executor = setup_executor();
    let ctx = ExprContext { row: RowDef { record: vec![] } };

    let inner = BoundExpr::Equals(
        ExprType::Bool,
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(1))),
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(1))),
    );

    let outer = BoundExpr::Equals(
        ExprType::Bool,
        Box::new(inner),
        Box::new(BoundExpr::Literal(ExprType::Bool, Literal::Bool(true))),
    );

    assert_eq!(
        executor.execute_expression(&outer, &ctx),
        Ok(Literal::Bool(true))
    );
}

#[test]
fn test_equals_column_error_propagation() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: RowDef { record: vec![] }
    };

    let expr = BoundExpr::Equals(
        ExprType::Bool,
        Box::new(BoundExpr::Column(ExprType::Int, 0)),
        Box::new(BoundExpr::Literal(ExprType::Int, Literal::Int(10))),
    );

    assert!(executor.execute_expression(&expr, &ctx).is_err());
}

#[test]
fn test_column_empty_row() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: RowDef { record: vec![] }
    };

    let expr = BoundExpr::Column(ExprType::Int, 0);
    assert!(executor.execute_expression(&expr, &ctx).is_err());
}