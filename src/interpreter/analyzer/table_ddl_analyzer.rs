use crate::compiler::ast::{ColumnDef, DataType, Literal, RowDef};
use crate::interpreter::analyzer::Analyzer;

impl Analyzer {

    pub fn analyze_create_table(&mut self, name: &str) -> Result<(), String> {
        let ctx = self.context.read().unwrap();

        // check there is no duplicate table name in current database
        let database = ctx.current_db.as_ref().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_some() {
            return Err(format!("The table '{}' already exists", name));
        }

        Ok(())
    }

    pub fn analyze_drop_table(&mut self, name: &str) -> Result<(), String> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_none() {
            return Err(format!("The table '{}' does not exists", name));
        }

        Ok(())
    }

    pub fn analyze_insert_table(&mut self, table: &str, rows: &Vec<RowDef>) -> Result<(), String> {
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

        Ok(())
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
        }
    }
}