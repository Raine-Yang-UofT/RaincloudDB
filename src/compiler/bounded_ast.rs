use crate::compiler::ast::{ColumnDef, ExprType, Literal};
use crate::types::ColumnId;

#[derive(Debug, Clone, PartialEq)]
pub struct BoundExprNode {
    pub expr_type: ExprType,
    pub expr: BoundExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    // binary
    Equals(Box<BoundExpr>, Box<BoundExpr>),
    Gt(Box<BoundExpr>, Box<BoundExpr>),
    Gte(Box<BoundExpr>, Box<BoundExpr>),
    Lt(Box<BoundExpr>, Box<BoundExpr>),
    Lte(Box<BoundExpr>, Box<BoundExpr>),
    NotEquals(Box<BoundExpr>, Box<BoundExpr>),
    And(Box<BoundExpr>, Box<BoundExpr>),
    Or(Box<BoundExpr>, Box<BoundExpr>),
    Add(Box<BoundExpr>, Box<BoundExpr>),
    Sub(Box<BoundExpr>, Box<BoundExpr>),
    Mul(Box<BoundExpr>, Box<BoundExpr>),
    Div(Box<BoundExpr>, Box<BoundExpr>),
    // unary
    Minus(Box<BoundExpr>),
    Not(Box<BoundExpr>),
    // primary
    Column(ColumnId),
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundStmt {
    CreateDatabase { name: String },
    DropDatabase { name: String },
    ConnectDatabase { name: String },
    DisconnectDatabase,

    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },

    DropTable { name: String },

    Insert {
        table: String,
        rows: Vec<Vec<BoundExprNode>>,
    },

    Update {
        table: String,
        assignments: Vec<BoundAssignment>,
        selection: Option<BoundExprNode>,
    },

    Select {
        table: String,
        columns: Vec<BoundExpr>,
        selection: Option<BoundExprNode>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAssignment {
    pub column_id: ColumnId,
    pub value: BoundExpr,
}