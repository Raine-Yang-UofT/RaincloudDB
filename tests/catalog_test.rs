use tempfile::TempDir;
use raincloud_db::compiler::ast::{ColumnDef, DataType};
use raincloud_db::interpreter::catalog::{Catalog, TableSchema};

fn setup_catalog() -> (TempDir, Catalog) {
    let dir = TempDir::new().unwrap();
    let catalog = Catalog::new(dir.path());
    (dir, catalog)
}

fn sample_table(name: &str) -> TableSchema {
    let columns = vec![
        ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ColumnDef { name: "name".to_string(), data_type: DataType::Char(5) },
    ];

    let column_index = columns.iter()
        .enumerate()
        .map(|(i, c)| (c.name.clone(), i))
        .collect();

    TableSchema {
        name: name.to_uppercase(),
        columns,
        column_index,
        first_page_id: 0,
    }
}

#[test]
fn test_create_database() {
    let (_dir, mut catalog) = setup_catalog();

    catalog.add_database("db1".to_string());

    assert!(catalog.has_database("db1"));
    assert!(catalog.has_database("DB1"));
}

#[test]
fn test_create_database_idempotent() {
    let (_dir, mut catalog) = setup_catalog();

    catalog.add_database("db1".to_string());
    catalog.add_database("db1".to_string());

    let dbs = catalog.list_databases();
    assert_eq!(dbs.len(), 1);
}

#[test]
fn test_remove_database() {
    let (_dir, mut catalog) = setup_catalog();

    catalog.add_database("db1".to_string());
    catalog.remove_database("db1");

    assert!(!catalog.has_database("db1"));
}

#[test]
fn test_database_persistence() {
    let dir = TempDir::new().unwrap();

    {
        let mut catalog = Catalog::new(dir.path());
        catalog.add_database("db1".to_string());
    }

    let catalog = Catalog::new(dir.path());
    assert!(catalog.has_database("db1"));
}

#[test]
fn test_add_table() {
    let (_dir, mut catalog) = setup_catalog();
    catalog.add_database("db1".to_string());

    let table = sample_table("users");
    catalog.add_table("db1", table).unwrap();

    assert!(catalog.has_table("db1", "users"));
}

#[test]
fn test_add_duplicate_table() {
    let (_dir, mut catalog) = setup_catalog();
    catalog.add_database("db1".to_string());

    let table = sample_table("users");
    catalog.add_table("db1", table.clone()).unwrap();
    let result = catalog.add_table("db1", table);

    assert!(result.is_err());
}

#[test]
fn test_remove_table() {
    let (_dir, mut catalog) = setup_catalog();
    catalog.add_database("db1".to_string());

    let table = sample_table("users");
    catalog.add_table("db1", table).unwrap();
    catalog.remove_table("db1", "users").unwrap();

    assert!(!catalog.has_table("db1", "users"));
}

#[test]
fn test_get_table_schema() {
    let (_dir, mut catalog) = setup_catalog();
    catalog.add_database("db1".to_string());

    let table = sample_table("users");
    catalog.add_table("db1", table.clone()).unwrap();

    let schema = catalog.get_table_schema("db1", "users").unwrap();
    assert_eq!(schema.name, "USERS");
    assert_eq!(schema.columns.len(), 2);
}

#[test]
fn test_table_persistence() {
    let dir = TempDir::new().unwrap();

    {
        let mut catalog = Catalog::new(dir.path());
        catalog.add_database("db1".to_string());

        let table = sample_table("users");
        catalog.add_table("db1", table).unwrap();
    }

    let catalog = Catalog::new(dir.path());

    assert!(catalog.has_table("db1", "users"));
}

#[test]
fn test_add_table_no_db() {
    let (_dir, mut catalog) = setup_catalog();

    let table = sample_table("users");
    let result = catalog.add_table("db1", table);

    assert!(result.is_err());
}

#[test]
fn test_table_case_normalization() {
    let (_dir, mut catalog) = setup_catalog();
    catalog.add_database("db1".to_string());

    let table = sample_table("Users"); // mixed case
    catalog.add_table("db1", table).unwrap();

    assert!(catalog.has_table("db1", "users"));
}