use crate::compiler::ast::Expression;
use crate::compiler::bounded_ast::BoundStmt;
use crate::interpreter::analyzer::Analyzer;
use crate::types::{DbError, DbResult};

impl Analyzer {

    pub fn analyze_select(
        &mut self,
        table: &str,
        column: &Vec<Expression>,
        selection: &Option<Expression>
    ) -> DbResult<BoundStmt> {
        let ctx = self.context.read().unwrap();

        // check the table exists in database
        let database = ctx.current_db.as_ref().unwrap();
        let schema = ctx.catalogs.get(database).unwrap().get_table_schema(table)
            .ok_or_else(|| DbError::TableNotFound(format!("Table '{}' does not exist", table)))?;

        // resolve column identifiers to column id
        let mut columns = Vec::new();
        for c in column {
            columns.push(self.analyze_expression(c, schema)?.expr);
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
            columns,
            selection: bound_selection
        })
    }
}