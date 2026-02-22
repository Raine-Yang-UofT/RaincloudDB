use std::collections::HashSet;
use crate::compiler::ast::{Assignment, ColumnDef, DataType, Expression, Literal, RowDef};
use crate::compiler::bounded_ast::{BoundAssignment, BoundStmt};
use crate::interpreter::analyzer::Analyzer;
use crate::types::ColumnId;

impl Analyzer {

    pub fn analyze_create_table(&mut self, name: &str, columns: &Vec<ColumnDef>) -> Result<BoundStmt, String> {
        let ctx = self.context.read().unwrap();

        // check there is no duplicate table name in current database
        let database = ctx.current_db.as_ref().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_some() {
            return Err(format!("The table '{}' already exists", name));
        }

        // check for duplicate column names
        let mut existing = HashSet::new();

        for col in columns {
            let name = &col.name;
            if !existing.insert(name) {
                return Err(format!("Duplicate column name '{}'", name));
            }
        }

        Ok(BoundStmt::CreateTable { name: String::from(name), columns: columns.clone() })
    }

    pub fn analyze_drop_table(&mut self, name: &str) -> Result<BoundStmt, String> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_none() {
            return Err(format!("The table '{}' does not exists", name));
        }

        Ok(BoundStmt::DropTable { name: String::from(name) })
    }

    pub fn analyze_insert_table(&mut self, table: &str, rows: &Vec<RowDef>) -> Result<BoundStmt, String> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        let schema = ctx.catalog.get_table_schema(database, table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;

        // check the rows match table schema
        for (row_index, row) in rows.iter().enumerate() {
            let record = &row.record;
            // check number of columns
            if record.len() != schema.columns.len() {
                return Err(format!(
                    "Row {} has {} values, but table '{}' expects {} columns",
                    row_index + 1, record.len(), table, schema.columns.len()
                ));
            }

            // check record data type
            for (col_index, (literal, column)) in record.iter()
                .zip(schema.columns.iter())
                .enumerate() {
                if let Err(err_msg) = self.validate_data_type(literal, column) {
                    return Err(format!(
                        "Type mismatch at Row {}, Column {} ('{}'): {}",
                        row_index + 1, col_index + 1, column.name, err_msg
                    ));
                }
            }
        }

        Ok(BoundStmt::Insert { table: String::from(table), rows: rows.clone() })
    }

    pub fn analyze_update_table(
        &mut self,
        table: &str,
        assignments: &Vec<Assignment>,
        selection: &Option<Expression>
    ) -> Result<BoundStmt, String> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        let schema = ctx.catalog.get_table_schema(database, table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;

        // check the update assignment is not empty
        if assignments.is_empty() {
            return Err("UPDATE must specify at least one column assignment".to_string());
        }

        let mut existing = HashSet::<ColumnId>::new();
        let mut bound_assignments = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            // resolve column name to column id
            let column_id = *schema.column_index
                .get(&assignment.column)
                .ok_or_else(|| format!("Column '{}' does not exist in {}", assignment.column, table))?;

            // check no duplicate columns
            if !existing.insert(column_id) {
                return Err(format!("Duplicate column '{}'", assignment.column));
            }

            // check data type compatibility
            let column_def = &schema.columns[column_id];
            self.validate_data_type(&assignment.value, column_def)?;

            // bind assignment
            bound_assignments.push(BoundAssignment {
                column_id,
                value: assignment.value.clone(),
            });
        }

        // bind WHERE clause
        let bound_selection = match selection {
            Some(expr) => {
                Some(self.analyze_where_clause(expr, schema)?)
            }
            None => None,
        };

        Ok(BoundStmt::Update {
            table: table.to_string(),
            assignments: bound_assignments,
            selection: bound_selection,
        })
    }

    fn validate_data_type(&self, literal: &Literal, column: &ColumnDef) -> Result<(), String> {
        match (&column.data_type, literal) {
            // validate INT type
            (DataType::Int, Literal::Int(_)) => Ok(()),
            (DataType::Int, _) => Err("expected INT".to_string()),
            // validate CHAR type
            (DataType::Char(expected_len), Literal::String(s)) => {
                let actual_len = s.len() as u32;
                if actual_len != *expected_len {
                    Err(format!(
                        "expected CHAR({}) but string is length {}",
                        expected_len, actual_len
                    ))
                } else {
                    Ok(())
                }
            },
            (DataType::Char(len), _) => Err(format!("expected CHAR({})", len)),
            _ => Err(format!("Unsupported data type at column {:?}", column).to_string()),
        }
    }
}