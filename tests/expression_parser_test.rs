use raincloud_db::compiler::ast::{BinaryOp, Expression, Literal, UnaryOp};
use raincloud_db::compiler::parser::Parser;
use raincloud_db::compiler::scanner::Scanner;

/// Helper that tokenizes & parses a single expression string into an Expression AST.
/// Adapt this if your parser API has a different method name.
fn parse(src: &str) -> Expression {
    let mut scanner = Scanner::new(src);
    let mut parser = Parser::new(&mut scanner);
    parser.parse_expression().expect(&format!("failed to parse: {}", src))
}

#[test]
fn test_simple_binary() {
    // 1 + 2
    let expr = parse("1 + 2");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Literal(Literal::Int(1))),
        op: BinaryOp::Add,
        rhs: Box::new(Expression::Literal(Literal::Int(2))),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_precedence_mul_over_add() {
    // 1 + 2 * 3  -> 1 + (2 * 3)
    let expr = parse("1 + 2 * 3");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Literal(Literal::Int(1))),
        op: BinaryOp::Add,
        rhs: Box::new(Expression::Binary {
            lhs: Box::new(Expression::Literal(Literal::Int(2))),
            op: BinaryOp::Mul,
            rhs: Box::new(Expression::Literal(Literal::Int(3))),
        }),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_parentheses_override_precedence() {
    // (1 + 2) * 3  -> (1 + 2) * 3
    let expr = parse("(1 + 2) * 3");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Binary {
            lhs: Box::new(Expression::Literal(Literal::Int(1))),
            op: BinaryOp::Add,
            rhs: Box::new(Expression::Literal(Literal::Int(2))),
        }),
        op: BinaryOp::Mul,
        rhs: Box::new(Expression::Literal(Literal::Int(3))),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_left_associative_subtraction() {
    // 1 - 2 - 3  -> (1 - 2) - 3
    let expr = parse("1 - 2 - 3");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Binary {
            lhs: Box::new(Expression::Literal(Literal::Int(1))),
            op: BinaryOp::Sub,
            rhs: Box::new(Expression::Literal(Literal::Int(2))),
        }),
        op: BinaryOp::Sub,
        rhs: Box::new(Expression::Literal(Literal::Int(3))),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_chained_comparisons_left_assoc() {
    // 1 < 2 < 3  -> (1 < 2) < 3  (most simple RD parsers are left-assoc here)
    let expr = parse("1 < 2 < 3");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Binary {
            lhs: Box::new(Expression::Literal(Literal::Int(1))),
            op: BinaryOp::Lt,
            rhs: Box::new(Expression::Literal(Literal::Int(2))),
        }),
        op: BinaryOp::Lt,
        rhs: Box::new(Expression::Literal(Literal::Int(3))),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_unary_and_binary_precedence() {
    // -1 + 2  -> (Neg 1) + 2
    let expr = parse("-1 + 2");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(Expression::Literal(Literal::Int(1))),
        }),
        op: BinaryOp::Add,
        rhs: Box::new(Expression::Literal(Literal::Int(2))),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_logical_not_precedence() {
    // NOT TRUE AND FALSE -> (NOT TRUE) AND FALSE
    let expr = parse("NOT TRUE AND FALSE");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expression::Literal(Literal::Bool(true))),
        }),
        op: BinaryOp::And,
        rhs: Box::new(Expression::Literal(Literal::Bool(false))),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_mixed_arith_cmp_logic() {
    // 1 + 2 > 3 AND NOT FALSE
    // -> ((1 + 2) > 3) AND (NOT FALSE)
    let expr = parse("1 + 2 > 3 AND NOT FALSE");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Binary {
            lhs: Box::new(Expression::Binary {
                lhs: Box::new(Expression::Literal(Literal::Int(1))),
                op: BinaryOp::Add,
                rhs: Box::new(Expression::Literal(Literal::Int(2))),
            }),
            op: BinaryOp::Gt,
            rhs: Box::new(Expression::Literal(Literal::Int(3))),
        }),
        op: BinaryOp::And,
        rhs: Box::new(Expression::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expression::Literal(Literal::Bool(false))),
        }),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_identifiers_and_grouping() {
    // a + b * (c - 1)  -> a + (b * (c - 1))
    let expr = parse("a + b * (c - 1)");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Identifier("A".to_string())),
        op: BinaryOp::Add,
        rhs: Box::new(Expression::Binary {
            lhs: Box::new(Expression::Identifier("B".to_string())),
            op: BinaryOp::Mul,
            rhs: Box::new(Expression::Binary {
                lhs: Box::new(Expression::Identifier("C".to_string())),
                op: BinaryOp::Sub,
                rhs: Box::new(Expression::Literal(Literal::Int(1))),
            }),
        }),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_complex_logical_arith_comparison() {
    // a OR b AND c = d + 5
    // common precedence: arithmetic > comparison > logical AND > logical OR
    // -> a OR ( b AND ( c = (d + 5) ) )
    let expr = parse("a OR b AND c = d + 5");
    let expected = Expression::Binary {
        lhs: Box::new(Expression::Identifier("A".to_string())),
        op: BinaryOp::Or,
        rhs: Box::new(Expression::Binary {
            lhs: Box::new(Expression::Identifier("B".to_string())),
            op: BinaryOp::And,
            rhs: Box::new(Expression::Binary {
                lhs: Box::new(Expression::Identifier("C".to_string())),
                op: BinaryOp::Eq,
                rhs: Box::new(Expression::Binary {
                    lhs: Box::new(Expression::Identifier("D".to_string())),
                    op: BinaryOp::Add,
                    rhs: Box::new(Expression::Literal(Literal::Int(5))),
                }),
            }),
        }),
    };
    assert_eq!(expr, expected);
}