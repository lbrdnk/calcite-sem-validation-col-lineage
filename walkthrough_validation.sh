#!/bin/bash

cat << 'EOF'
====================================================================
Apache Calcite SQL Validation Example - Code Walkthrough
====================================================================

This example shows how to validate SQL queries against a data source.

STEP 1: Define Your Schema
---------------------------
You need to tell Calcite about your tables and columns.

Option A - Custom Schema (programmatic):
```java
class EmployeesTable extends AbstractTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("EMPLOYEE_ID", SqlTypeName.INTEGER)
            .add("FIRST_NAME", SqlTypeName.VARCHAR, 50)
            .add("LAST_NAME", SqlTypeName.VARCHAR, 50)
            .add("EMAIL", SqlTypeName.VARCHAR, 100)
            .add("DEPARTMENT_ID", SqlTypeName.INTEGER)
            .build();
    }
}
```

Option B - JDBC Schema (from actual database):
```java
DataSource dataSource = // your database connection
JdbcSchema jdbcSchema = JdbcSchema.create(
    null,
    dataSource,
    "your_schema_name",
    null
);
// This automatically reads all tables and columns from the database!
```

STEP 2: Create the Catalog Reader
----------------------------------
The catalog reader provides Calcite with metadata access:

```java
// Create root schema
CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
rootSchema.add("MY_SCHEMA", yourSchemaObject);

// Create catalog reader
CalciteCatalogReader catalogReader = new CalciteCatalogReader(
    rootSchema,
    Collections.singletonList("MY_SCHEMA"),  // default schema path
    typeFactory,
    connectionConfig
);
```

STEP 3: Create the Validator
-----------------------------
```java
SqlValidator validator = SqlValidatorUtil.newValidator(
    SqlStdOperatorTable.instance(),  // SQL operators (=, +, COUNT, etc.)
    catalogReader,                   // Your schema metadata
    typeFactory,                     // Type system
    validatorConfig                  // Validation rules
);
```

STEP 4: Parse and Validate SQL
-------------------------------
```java
// Parse SQL string to AST
SqlParser parser = SqlParser.create(sql);
SqlNode sqlNode = parser.parseQuery();

// Validate against schema
SqlNode validatedNode = validator.validate(sqlNode);

// If validation succeeds, the query is valid!
// If it fails, you get a detailed error message
```

WHAT GETS VALIDATED?
---------------------
✓ Table names exist in the schema
✓ Column names exist in their tables
✓ Data types are compatible in expressions
✓ Aggregate functions are used correctly
✓ JOIN conditions reference valid columns
✓ GROUP BY columns are valid
✓ Subqueries are well-formed
✓ Function calls have correct arguments

EXAMPLE VALIDATION SCENARIOS
-----------------------------

Query: SELECT employee_id, first_name FROM employees
Result: ✓ Valid (all columns exist)

Query: SELECT invalid_column FROM employees
Result: ✗ Error: Column 'INVALID_COLUMN' not found in table 'EMPLOYEES'

Query: SELECT * FROM non_existent_table
Result: ✗ Error: Object 'NON_EXISTENT_TABLE' not found

Query: SELECT COUNT(*) FROM employees WHERE salary > 'not_a_number'
Result: ✗ Error: Cannot apply '>' to arguments of type '<DECIMAL> > <VARCHAR>'

Query: SELECT department_id FROM employees GROUP BY first_name
Result: ✗ Error: Expression 'DEPARTMENT_ID' is not being grouped

COMPLETE WORKING EXAMPLE
-------------------------
See the files in this directory:
- SqlValidationExample.java: Custom schema validation
- JdbcValidationExample.java: JDBC database validation
- CompanySchema.java: Example schema definition

To run with Maven:
  mvn clean compile
  mvn exec:java -Dexec.mainClass="com.example.SqlValidationExample"
  mvn exec:java -Dexec.mainClass="com.example.JdbcValidationExample"

USING WITH REAL DATABASES
--------------------------

PostgreSQL:
```java
DataSource ds = ... // Your PostgreSQL connection pool
JdbcSchema schema = JdbcSchema.create(null, ds, "public", null);
```

MySQL:
```java
DataSource ds = ... // Your MySQL connection pool
JdbcSchema schema = JdbcSchema.create(null, ds, "mydb", null);
```

Oracle:
```java
DataSource ds = ... // Your Oracle connection pool
JdbcSchema schema = JdbcSchema.create(null, ds, "MYSCHEMA", null);
```

WHY USE CALCITE FOR VALIDATION?
--------------------------------
1. Catch errors before execution (saves time & resources)
2. Provide better error messages to users
3. Build query builders with real-time validation
4. Create SQL IDEs/editors with intelligent suggestions
5. Validate queries in CI/CD pipelines
6. Test that code references valid database objects

MAVEN DEPENDENCIES NEEDED
--------------------------
<dependency>
    <groupId>org.apache.calcite</groupId>
    <artifactId>calcite-core</artifactId>
    <version>1.37.0</version>
</dependency>

For JDBC validation, also add your database driver:
- PostgreSQL: org.postgresql:postgresql
- MySQL: mysql:mysql-connector-java
- Oracle: com.oracle.database.jdbc:ojdbc8
- H2: com.h2database:h2

====================================================================
EOF
