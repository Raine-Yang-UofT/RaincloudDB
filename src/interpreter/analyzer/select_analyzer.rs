use crate::compiler::ast::Expression;
use crate::compiler::bounded_ast::BoundStmt;
use crate::interpreter::analyzer::Analyzer;
use crate::types::{DbError, DbResult};

impl Analyzer {

    pub fn analyze_select(
        &mut self,
        table: &str,
        column: &Vec<String>,
        selection: &Option<Expression>
    ) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        let schema = ctx.catalog.get_table_schema(database, table)
            .ok_or_else(|| DbError::TableNotFound(format!("Table '{}' does not exist", table)))?;

        // resolve column identifiers to column id
        let mut column_ids = Vec::new();
        for c in column {
            let column_id = *schema.column_index
                .get(c)
                .ok_or_else(|| DbError::ColumnNotFound(format!("Column '{}' does not exist in {}", c, table)))?;
            column_ids.push(column_id);
        }

        // analyze condition expression
        let bound_selection = match selection {
            Some(expr) => {
                Some(self.analyze_where_clause(expr, schema)?)
            }
            None => None,
        };

        Ok(BoundStmt::Select {
            table: String::from(table),
            columns: column_ids,
            selection: bound_selection
        })
    }
}