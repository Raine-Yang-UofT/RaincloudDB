## SQL Support Specification (v0)

This document defines the SQL syntax and semantics supported by the current version of RaincloudDB. The goal is to describe a minimal, consistent SQL subset that maps directly to the query compiler and execution layer currently being built.

This specification is forward-looking: it describes the features that are expected to function after completing the planned query engine tasks.

The implementation intentionally excludes many advanced SQL features for now. Only the constructs listed here are supported.

### Overview

Supported categories:
- Database management
- Schema definition
- Table operations
- Basic insert and update
- Basic selection, projection, and predicates
- Minimal expression support
  Database Management Statements

### Database Management

#### CREATE DATABASE
Create a new database. Creates catalog and data storage metadata.

```sql
CREATE DATABASE database_name;
```

#### DROP DATABASE

Delete an existing database. Removes catalog and stored tables.

```sql
DROP DATABASE database_name;
```

### DDL: Schema Definition
#### CREATE TABLE

Create a table with named columns and primitive data types.

```sql
CREATE TABLE table_name (
    column_name data_type,
);
```
Supported data types:

| Data type | Internal type	 | Notes         |
|-----------|----------------|---------------|
| INT       | integer        | Stored as i32 | 
| CHAR(int)   | String         | Fixed Length  |


Example:

```sql
CREATE TABLE users (
  id INT,
  name CHAR(10)
);
```

#### DROP TABLE

Drop a table and delete its data.

```sql
DROP TABLE table_name;
```

### DML: Data Manipulation
#### INSERT

Insert a full row into a table:
```sql
INSERT INTO table_name VALUES (value1, value2, ...);
```
Rules:
- Number of values must match number of columns
- No column-name list syntax yet

#### UPDATE
Update rows matching a simple predicate.

```sql
UPDATE table_name
SET column_name = value
WHERE column_name = value;
```

### SELECT Queries
Basic SELECT
Project one or more columns and scan table.
```sql
SELECT column1, column2 FROM table_name;
```

WHERE Clause

Simple equality filter:
```sql
SELECT name FROM users WHERE id = 1;
```
Supported predicates:
- column = literal
