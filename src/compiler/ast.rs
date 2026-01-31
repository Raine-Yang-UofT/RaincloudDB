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
        columns: Vec<String>,
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

/// Row Definition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDef {
    pub record: Vec<Literal>,
}

/// Column Data Type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Char(u32), // CHAR(n)
}

/// Expression
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression {
    Equals(Box<Expression>, Box<Expression>),
    Identifier(String),
    Literal(Literal),
}

/// Expression Data Type
#[derive(Debug, Clone, PartialEq)]
pub enum ExprType {
    Int,
    Char,
    Bool,
}

/// Literal
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Int(i32),
    String(String),
    Bool(bool),
}

/// Assignment statement in update
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: String,
    pub value: Literal,
}

// AST node methods
impl RowDef {

    // convert record to raw bytes
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

    // deserialize record with given schema
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