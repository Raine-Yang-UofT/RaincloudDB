mod common;

use raincloud_db::compiler::ast::{Literal, Record};
use raincloud_db::compiler::bounded_ast::BoundExpr;
use raincloud_db::interpreter::executor::{Executor, ExprContext};
use crate::common::setup_interpreter;

fn setup_executor() -> Executor {
    let interpreter = setup_interpreter();
    Executor::new(interpreter.context)
}

#[test]
fn test_literal() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };

    let expr1 = BoundExpr::Literal(Literal::Int(10));
    let expr2 = BoundExpr::Literal(Literal::String("Hello World!".to_string()));
    let expr3 = BoundExpr::Literal(Literal::Bool(true));
    let expr4 = BoundExpr::Literal(Literal::Int(-42));

    assert_eq!(executor.execute_expression(&expr1, &ctx).unwrap(), Literal::Int(10));
    assert_eq!(executor.execute_expression(&expr2, &ctx).unwrap(), Literal::String("Hello World!".to_string()));
    assert_eq!(executor.execute_expression(&expr3, &ctx).unwrap(), Literal::Bool(true));
    assert_eq!(executor.execute_expression(&expr4, &ctx).unwrap(), Literal::Int(-42));
}

#[test]
fn test_column_access() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: Some(&Record {
            record: vec![
                Literal::Int(25),
                Literal::String("Alice".to_string()),
            ]
        })
    };
    assert_eq!(
        executor.execute_expression(&BoundExpr::Column(0), &ctx).unwrap(),
        Literal::Int(25)
    );
    assert_eq!(
        executor.execute_expression(&BoundExpr::Column(1), &ctx).unwrap(),
        Literal::String("Alice".to_string())
    );
}

#[test]
fn test_column_out_of_bounds() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: Some(&Record {
            record: vec![Literal::Int(10)]
        })
    };
    assert!(executor.execute_expression(&BoundExpr::Column(5), &ctx).is_err());
}

#[test]
fn test_equals_literals_true() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };
    let expr = BoundExpr::Equals(
        Box::new(BoundExpr::Literal(Literal::Int(10))),
        Box::new(BoundExpr::Literal(Literal::Int(10))),
    );
    assert_eq!(
        executor.execute_expression(&expr, &ctx).unwrap(),
        Literal::Bool(true)
    );
}

#[test]
fn test_equals_literals_false() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };
    let expr = BoundExpr::Equals(
        Box::new(BoundExpr::Literal(Literal::Int(10))),
        Box::new(BoundExpr::Literal(Literal::Int(20))),
    );
    assert_eq!(
        executor.execute_expression(&expr, &ctx).unwrap(),
        Literal::Bool(false)
    );
}

#[test]
fn test_equals_column_literal() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: Some(&Record {
            record: vec![Literal::Int(30)]
        })
    };
    let expr = BoundExpr::Equals(
        Box::new(BoundExpr::Column(0)),
        Box::new(BoundExpr::Literal(Literal::Int(30))),
    );
    assert_eq!(
        executor.execute_expression(&expr, &ctx).unwrap(),
        Literal::Bool(true)
    );
}

#[test]
fn test_equals_column_column() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: Some(&Record {
            record: vec![
                Literal::Int(10),
                Literal::Int(10),
            ]
        })
    };
    let expr = BoundExpr::Equals(
        Box::new(BoundExpr::Column(0)),
        Box::new(BoundExpr::Column(1)),
    );
    assert_eq!(
        executor.execute_expression(&expr, &ctx).unwrap(),
        Literal::Bool(true)
    );
}

#[test]
fn test_nested_equals() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };
    let inner = BoundExpr::Equals(
        Box::new(BoundExpr::Literal(Literal::Int(1))),
        Box::new(BoundExpr::Literal(Literal::Int(1))),
    );
    let outer = BoundExpr::Equals(
        Box::new(inner),
        Box::new(BoundExpr::Literal(Literal::Bool(true))),
    );
    assert_eq!(
        executor.execute_expression(&outer, &ctx).unwrap(),
        Literal::Bool(true)
    );
}

#[test]
fn test_basic_arithmetic() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };

    let add = BoundExpr::Add(
        Box::new(BoundExpr::Literal(Literal::Int(2))),
        Box::new(BoundExpr::Literal(Literal::Int(3))),
    );
    let sub = BoundExpr::Sub(
        Box::new(BoundExpr::Literal(Literal::Int(10))),
        Box::new(BoundExpr::Literal(Literal::Int(3))),
    );
    let mul = BoundExpr::Mul(
        Box::new(BoundExpr::Literal(Literal::Int(4))),
        Box::new(BoundExpr::Literal(Literal::Int(5))),
    );
    let div = BoundExpr::Div(
        Box::new(BoundExpr::Literal(Literal::Int(20))),
        Box::new(BoundExpr::Literal(Literal::Int(4))),
    );

    assert_eq!(executor.execute_expression(&add, &ctx).unwrap(), Literal::Int(5));
    assert_eq!(executor.execute_expression(&sub, &ctx).unwrap(), Literal::Int(7));
    assert_eq!(executor.execute_expression(&mul, &ctx).unwrap(), Literal::Int(20));
    assert_eq!(executor.execute_expression(&div, &ctx).unwrap(), Literal::Int(5));
}

#[test]
fn test_unary_minus() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };
    let expr = BoundExpr::Minus(Box::new(BoundExpr::Literal(Literal::Int(10))));
    assert_eq!(executor.execute_expression(&expr, &ctx).unwrap(), Literal::Int(-10));
}

#[test]
fn test_comparison() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };

    let gt = BoundExpr::Gt(
        Box::new(BoundExpr::Literal(Literal::Int(5))),
        Box::new(BoundExpr::Literal(Literal::Int(3))),
    );
    let lt = BoundExpr::Lt(
        Box::new(BoundExpr::Literal(Literal::Int(2))),
        Box::new(BoundExpr::Literal(Literal::Int(7))),
    );
    let gte = BoundExpr::Gte(
        Box::new(BoundExpr::Literal(Literal::Int(5))),
        Box::new(BoundExpr::Literal(Literal::Int(5))),
    );
    let lte = BoundExpr::Lte(
        Box::new(BoundExpr::Literal(Literal::Int(3))),
        Box::new(BoundExpr::Literal(Literal::Int(3))),
    );
    let ne = BoundExpr::NotEquals(
        Box::new(BoundExpr::Literal(Literal::Int(1))),
        Box::new(BoundExpr::Literal(Literal::Int(2))),
    );

    assert_eq!(executor.execute_expression(&gt, &ctx).unwrap(), Literal::Bool(true));
    assert_eq!(executor.execute_expression(&lt, &ctx).unwrap(), Literal::Bool(true));
    assert_eq!(executor.execute_expression(&gte, &ctx).unwrap(), Literal::Bool(true));
    assert_eq!(executor.execute_expression(&lte, &ctx).unwrap(), Literal::Bool(true));
    assert_eq!(executor.execute_expression(&ne, &ctx).unwrap(), Literal::Bool(true));
}

#[test]
fn test_boolean() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };

    let and_expr = BoundExpr::And(
        Box::new(BoundExpr::Literal(Literal::Bool(true))),
        Box::new(BoundExpr::Literal(Literal::Bool(false))),
    );
    let or_expr = BoundExpr::Or(
        Box::new(BoundExpr::Literal(Literal::Bool(true))),
        Box::new(BoundExpr::Literal(Literal::Bool(false))),
    );
    let not_expr = BoundExpr::Not(
        Box::new(BoundExpr::Literal(Literal::Bool(false)))
    );

    assert_eq!(executor.execute_expression(&and_expr, &ctx).unwrap(), Literal::Bool(false));
    assert_eq!(executor.execute_expression(&or_expr, &ctx).unwrap(), Literal::Bool(true));
    assert_eq!(executor.execute_expression(&not_expr, &ctx).unwrap(), Literal::Bool(true));
}

#[test]
fn test_complex_expression() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: Some(&Record {
            record: vec![Literal::Int(7)]
        })
    };

    let add = BoundExpr::Add(
        Box::new(BoundExpr::Column(0)),
        Box::new(BoundExpr::Literal(Literal::Int(5))),
    );
    let gt = BoundExpr::Gt(
        Box::new(add),
        Box::new(BoundExpr::Literal(Literal::Int(10))),
    );
    let not = BoundExpr::Not(
        Box::new(BoundExpr::Literal(Literal::Bool(false)))
    );
    let and = BoundExpr::And(
        Box::new(gt),
        Box::new(not),
    );

    assert_eq!(executor.execute_expression(&and, &ctx).unwrap(), Literal::Bool(true));
}

#[test]
fn test_divide_by_zero() {
    let executor = setup_executor();
    let ctx = ExprContext { row: Some(&Record { record: vec![] }) };

    let expr = BoundExpr::Div(
        Box::new(BoundExpr::Literal(Literal::Int(10))),
        Box::new(BoundExpr::Literal(Literal::Int(0))),
    );

    assert!(executor.execute_expression(&expr, &ctx).is_err());
}

#[test]
fn test_equals_column_error_propagation() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: Some(&Record { record: vec![] })
    };
    let expr = BoundExpr::Equals(
        Box::new(BoundExpr::Column(0)),
        Box::new(BoundExpr::Literal(Literal::Int(10))),
    );
    assert!(executor.execute_expression(&expr, &ctx).is_err());
}

#[test]
fn test_column_empty_row() {
    let executor = setup_executor();
    let ctx = ExprContext {
        row: Some(&Record { record: vec![] })
    };
    let expr = BoundExpr::Column(0);
    assert!(executor.execute_expression(&expr, &ctx).is_err());
}