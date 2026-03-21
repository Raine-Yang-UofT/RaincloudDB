use std::collections::HashSet;
use crate::compiler::ast::{Assignment, ColumnDef, Expression};
use crate::compiler::bounded_ast::{BoundAssignment, BoundStmt};
use crate::interpreter::analyzer::Analyzer;
use crate::types::{ColumnId, DbError, DbResult};

impl Analyzer {

    pub fn analyze_create_table(&mut self, name: &str, columns: &Vec<ColumnDef>) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check there is no duplicate table name in current database
        let database = ctx.current_db.as_ref().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_some() {
            return Err(DbError::DuplicateTable(format!("The table '{}' already exists", name)));
        }

        // check for duplicate column names
        let mut existing = HashSet::new();

        for col in columns {
            let name = &col.name;
            if !existing.insert(name) {
                return Err(DbError::DuplicateColumn(format!("Duplicate column name '{}'", name)));
            }
        }

        Ok(BoundStmt::CreateTable { name: String::from(name), columns: columns.clone() })
    }

    pub fn analyze_drop_table(&mut self, name: &str) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        if ctx.catalog.get_table_schema(&database, name).is_none() {
            return Err(DbError::TableNotFound(format!("The table '{}' does not exists", name)));
        }

        Ok(BoundStmt::DropTable { name: String::from(name) })
    }

    pub fn analyze_insert_table(&mut self, table: &str, rows: &Vec<Vec<Expression>>) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        let schema = ctx.catalog.get_table_schema(database, table)
            .ok_or_else(|| DbError::TableNotFound(format!("Table '{}' does not exist", table)))?;

        // check the rows match table schema
        let mut bounded_rows = vec!();
        for (row_index, row) in rows.iter().enumerate() {
            // check number of columns
            if row.len() != schema.columns.len() {
                return Err(DbError::ColumnMismatch(
                    format!("Row {} has {} values, but table '{}' expects {} columns",
                            row_index + 1, row.len(), table, schema.columns.len()
                )));
            }

            // check record data type
            let mut bounded_row = vec!();
            for (col_index, (expr, column)) in row.iter()
                .zip(schema.columns.iter())
                .enumerate() {
                let bound_expr = self.analyze_expression(expr, schema)?;
                if column.data_type.check_type(&bound_expr.expr_type) {
                    bounded_row.push(bound_expr);
                } else {
                    return Err(DbError::TypeMismatch(
                        format!("Expect type {:?}; Got {:?} at Row {}, Column {} ('{}')",
                                column.data_type, bound_expr.expr_type, row_index + 1, col_index + 1, column.name
                        )));
                }

            }
            bounded_rows.push(bounded_row);
        }

        Ok(BoundStmt::Insert { table: String::from(table), rows: bounded_rows })
    }

    pub fn analyze_update_table(
        &mut self,
        table: &str,
        assignments: &Vec<Assignment>,
        selection: &Option<Expression>
    ) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        let schema = ctx.catalog.get_table_schema(database, table)
            .ok_or_else(|| DbError::TableNotFound(format!("Table '{}' does not exist", table)))?;

        // check the update assignment is not empty
        if assignments.is_empty() {
            return Err(DbError::ExpressionNotFound("UPDATE must specify at least one column assignment".to_string()));
        }

        let mut existing = HashSet::<ColumnId>::new();
        let mut bound_assignments = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            // resolve column name to column id
            let column_id = *schema.column_index
                .get(&assignment.column)
                .ok_or_else(|| DbError::ColumnNotFound(format!("Column '{}' does not exist in {}", assignment.column, table)))?;

            // check no duplicate columns
            if !existing.insert(column_id) {
                return Err(DbError::DuplicateColumn(format!("Duplicate column '{}'", assignment.column)));
            }

            // check update expression and data type compatibility
            let upd_expr = self.analyze_expression(&assignment.value, schema)?;
            let column_def = &schema.columns[column_id];
            if !column_def.data_type.check_type(&upd_expr.expr_type) {
                return Err(DbError::TypeMismatch(
                    format!("The expression evaluates to a different data type than column {:?}", column_def)));
            }

            // bind assignment
            bound_assignments.push(BoundAssignment {
                column_id,
                value: upd_expr.expr,
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
}