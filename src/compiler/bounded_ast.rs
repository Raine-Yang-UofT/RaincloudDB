use crate::compiler::ast::{ColumnDef, ExprType, Literal, RowDef};
use crate::types::ColumnId;

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    Equals(ExprType, Box<BoundExpr>, Box<BoundExpr>),
    Column(ExprType, ColumnId),
    Literal(ExprType, Literal),
}

impl BoundExpr {
    pub fn get_type(&self) -> &ExprType {
        match self {
            BoundExpr::Equals(expr_type, _, _) => expr_type,
            BoundExpr::Column(expr_type, _) => expr_type,
            BoundExpr::Literal(expr_type, _) => expr_type,
        }
    }
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
        rows: Vec<RowDef>,
    },

    Update {
        table: String,
        assignments: Vec<BoundAssignment>,
        selection: Option<BoundExpr>,
    },

    Select {
        table: String,
        columns: Vec<ColumnId>,
        selection: Option<BoundExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAssignment {
    pub column_id: ColumnId,
    pub value: Literal,
}