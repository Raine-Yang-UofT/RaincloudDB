#[derive(Debug, Clone, PartialEq)]
pub enum Statement {

    CreateDatabase { name: String },
    DropDatabase { name: String },
    UseDatabase { name: String },

    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable { name: String },

    Insert {
        table: String,
        values: Vec<Literal>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Char(u32), // CHAR(n)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Equals(Box<Expression>, Box<Expression>),
    Identifier(String),
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i32),
    String(String),
}

/// Assignment statement in update
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Literal,
}