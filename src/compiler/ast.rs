use std::cmp::Ordering;
use std::fmt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {

    CreateDatabase { name: String },
    DropDatabase { name: String },
    ConnectDatabase { name: String },
    DisconnectDatabase { },

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
        assignments: Vec<Assignment>,
        selection: Option<Expression>, // WHERE clause
    },

    Select {
        columns: Vec<Expression>,
        table: String,
        selection: Option<Expression>, // WHERE clause
    },
}

/// Column Definition
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Column Data Type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Char(u32), // CHAR(n)
}

impl DataType {
    pub fn check_type(&self, expr: &ExprType) -> bool {
        match self {
            DataType::Int => *expr == ExprType::Int,
            DataType::Char(_) => *expr == ExprType::Char,
        }
    }
}

/// Expression
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression {
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    Binary {
        lhs: Box<Expression>,
        op: BinaryOp,
        rhs: Box<Expression>,
    },
    Identifier(String),
    Literal(Literal),
}

/// Expression Data Type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprType {
    Int,
    Char,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp {
    Or, And, Eq, NotEq,
    Gt, Gte, Lt, Lte,
    Add, Sub, Mul, Div,
}

/// Literal
#[derive(Debug, Clone)]
pub enum Literal {
    Int(i32),
    String(String),
    Bool(bool),
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Int(a), Literal::Int(b)) => a == b,
            (Literal::String(a), Literal::String(b)) => a == b,
            (Literal::Bool(a), Literal::Bool(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Literal {}

impl PartialOrd for Literal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Literal::Int(a), Literal::Int(b)) => a.partial_cmp(b),
            (Literal::String(a), Literal::String(b)) => a.partial_cmp(b),
            (Literal::Bool(a), Literal::Bool(b)) => a.partial_cmp(b),
            _ => None, // different types are not comparable
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Int(v) => write!(f, "{v}"),
            Literal::String(v) => write!(f, "'{v}'"),
            Literal::Bool(true) => write!(f, "TRUE"),
            Literal::Bool(false) => write!(f, "FALSE"),
        }
    }
}

/// Assignment statement in update
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

/// Row Definition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDef {
    pub record: Vec<Literal>,
}

// AST node methods
impl RowDef {

    /// Convert record to raw bytes
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        let mut buf = Vec::new();

        for value in &self.record {
            match value {
                Literal::Int(v) => {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
                Literal::String(v) => {
                    buf.extend_from_slice(&v.as_bytes());
                }
                Literal::Bool(v) => {
                    buf.extend_from_slice(&[*v as u8]);
                }
            }
        }

        Ok(buf)
    }

    /// Deserialize record with given schema
    pub fn deserialize(buf: &[u8], schema: &Vec<ColumnDef>) -> Result<Self, String> {
        let mut offset = 0;
        let mut record = Vec::with_capacity(schema.len());

        for col in schema {
            match col.data_type {
                DataType::Int => {
                    if offset + 4 > buf.len() {
                        return Err("Unexpected end while reading INT".to_string());
                    }

                    let v = i32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    record.push(Literal::Int(v));
                }
                DataType::Char(v) => {
                    let length = v as usize;
                    if offset + length > buf.len() {
                        return Err("Unexpected end while reading CHAR".to_string());
                    }

                    let bytes = &buf[offset..offset + length];
                    let s = String::from_utf8_lossy(bytes).trim_end_matches('\0').to_string();
                    offset += length;
                    record.push(Literal::String(s));
                }
            }
        }

        Ok(RowDef { record })
    }
}