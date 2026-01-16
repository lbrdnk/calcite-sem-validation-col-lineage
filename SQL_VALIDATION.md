# SQL Validation with Apache Calcite

This guide shows how to validate SQL queries against a schema using Calcite.

## Overview

SQL validation ensures that a query is semantically correct before execution:
1. **Table Existence**: All referenced tables exist in the schema
2. **Column Existence**: All referenced columns exist in their respective tables
3. **Type Compatibility**: Expressions use compatible data types
4. **Semantic Correctness**: GROUP BY, aggregates, JOINs are well-formed

## Quick Example

```java
// Parse SQL string to AST
SqlParser parser = SqlParser.create(sql);
SqlNode sqlNode = parser.parseQuery();

// Create validator with schema
SqlValidator validator = createValidator();

// Validate - throws exception if invalid
SqlNode validatedNode = validator.validate(sqlNode);

// Get result type (column names and types)
RelDataType resultType = validator.getValidatedNodeType(validatedNode);
System.out.println("Result: " + resultType);
```

## Key Components

### 1. `SqlParser` - Parse SQL to AST

Converts SQL string into an Abstract Syntax Tree (SqlNode):

```java
SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
SqlNode sqlNode = parser.parseQuery();
```

**Throws:** `SqlParseException` for syntax errors

**Example Errors:**
```
SELECT FROM table     -> Parse error: Encountered "FROM" at line 1
SELECT * FORM table   -> Parse error: Encountered "FORM" at line 1 (typo)
```

### 2. `SqlValidator` - Validate Against Schema

The core validation component that checks semantic correctness:

```java
SqlValidator validator = SqlValidatorUtil.newValidator(
    SqlStdOperatorTable.instance(),  // SQL operators (+, =, COUNT, etc.)
    catalogReader,                   // Schema metadata provider
    typeFactory,                     // Type system
    validatorConfig                  // Validation settings
);

SqlNode validatedNode = validator.validate(sqlNode);
```

**Throws:** `CalciteContextException` for validation errors

**What Gets Validated:**
- Table names exist in schema
- Column names exist in tables
- Data types are compatible
- Aggregate functions used correctly
- GROUP BY columns are valid
- JOIN conditions are valid
- Subqueries are well-formed

### 3. `CalciteCatalogReader` - Schema Metadata Provider

Provides table and column metadata to the validator:

```java
CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
rootSchema.add("MYSCHEMA", schemaObject);

CalciteCatalogReader catalogReader = new CalciteCatalogReader(
    rootSchema,
    Collections.singletonList("MYSCHEMA"),  // Default schema path
    typeFactory,
    connectionConfig
);
```

### 4. `RelDataTypeFactory` - Type System

Handles SQL type definitions and conversions:

```java
RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
```

## Schema Definition Approaches

### Approach A: Custom Schema (Programmatic)

Define schema in code when you don't have an actual database:

```java
public class MySchema extends AbstractSchema {
    @Override
    public Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        tables.put("EMPLOYEES", new EmployeesTable());
        tables.put("DEPARTMENTS", new DepartmentsTable());
        return tables;
    }
}

class EmployeesTable extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("EMPLOYEE_ID", SqlTypeName.INTEGER)
            .add("FIRST_NAME", SqlTypeName.VARCHAR, 50)
            .add("LAST_NAME", SqlTypeName.VARCHAR, 50)
            .add("SALARY", SqlTypeName.DECIMAL, 10, 2)
            .add("DEPARTMENT_ID", SqlTypeName.INTEGER)
            .build();
    }
}
```

### Approach B: JDBC Schema (From Database)

Read schema directly from an existing database:

```java
// Create root schema
CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

// Create JDBC schema - reads metadata from database
JdbcSchema jdbcSchema = JdbcSchema.create(
    rootSchema.plus(),   // Parent schema
    "PUBLIC",            // Schema name in Calcite
    dataSource,          // JDBC DataSource
    null,                // Catalog (null for default)
    "public"             // Database schema name
);

rootSchema.add("PUBLIC", jdbcSchema);
```

**Database-Specific Schema Names:**
- PostgreSQL: `"public"` (default schema)
- MySQL: `"your_database"` (database name)
- Oracle: `"YOUR_SCHEMA"` (schema name in uppercase)
- H2: `null` (reads all schemas)

## Validator Configuration

```java
SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
    .withIdentifierExpansion(true)      // Expand * to column list
    .withSqlConformance(SqlConformanceEnum.DEFAULT);  // SQL standard compliance
```

**Key Configuration Options:**
- `withIdentifierExpansion(true)` - Expands `SELECT *` to actual columns
- `withSqlConformance(...)` - Sets SQL dialect (DEFAULT, LENIENT, STRICT, etc.)
- `withColumnReferenceExpansion(true)` - Expands column references

## Error Handling

```java
try {
    SqlNode sqlNode = parser.parseQuery();
    SqlNode validated = validator.validate(sqlNode);
} catch (SqlParseException e) {
    // Syntax error - SQL is malformed
    System.out.println("Parse error: " + e.getMessage());
} catch (CalciteContextException e) {
    // Validation error - SQL is syntactically correct but semantically wrong
    System.out.println("Validation error: " + e.getMessage());
}
```

**Note:** `CalciteContextException` is in `org.apache.calcite.runtime` package (Calcite 1.37.0+).

## Validation Error Examples

### Invalid Column
```sql
SELECT invalid_column FROM EMPLOYEES
```
```
Validation error: Column 'INVALID_COLUMN' not found in any table
```

### Invalid Table
```sql
SELECT * FROM non_existent_table
```
```
Validation error: Object 'NON_EXISTENT_TABLE' not found
```

### Type Mismatch
```sql
SELECT * FROM EMPLOYEES WHERE salary > 'not_a_number'
```
```
Validation error: Cannot apply '>' to arguments of type '<DECIMAL(10,2)> > <CHAR(12)>'
```

### Invalid GROUP BY
```sql
SELECT department_id, first_name FROM EMPLOYEES GROUP BY department_id
```
```
Validation error: Expression 'FIRST_NAME' is not being grouped
```

### Invalid Aggregate
```sql
SELECT COUNT(*) + first_name FROM EMPLOYEES
```
```
Validation error: Expression 'FIRST_NAME' is not being grouped
```

### Invalid JOIN Condition
```sql
SELECT * FROM EMPLOYEES E JOIN DEPARTMENTS D ON E.invalid_col = D.department_id
```
```
Validation error: Column 'INVALID_COL' not found in table 'E'
```

## Use Cases

### Use Case 1: Query Validation API

Validate user-submitted queries before execution:

```java
public ValidationResult validateUserQuery(String sql) {
    try {
        SqlNode sqlNode = parser.parseQuery(sql);
        SqlNode validated = validator.validate(sqlNode);
        RelDataType resultType = validator.getValidatedNodeType(validated);

        return ValidationResult.success(resultType);
    } catch (SqlParseException e) {
        return ValidationResult.syntaxError(e.getMessage());
    } catch (CalciteContextException e) {
        return ValidationResult.validationError(e.getMessage());
    }
}
```

### Use Case 2: IDE/Editor Support

Provide real-time SQL validation in code editors:

```java
public List<SqlError> validateForEditor(String sql) {
    List<SqlError> errors = new ArrayList<>();
    try {
        SqlNode sqlNode = parser.parseQuery(sql);
        validator.validate(sqlNode);
    } catch (CalciteContextException e) {
        // Extract position information for editor highlighting
        int line = e.getPosLine();
        int column = e.getPosColumn();
        errors.add(new SqlError(line, column, e.getMessage()));
    }
    return errors;
}
```

### Use Case 3: Migration Testing

Verify queries work with new schema before migration:

```java
public void validateQueriesAgainstNewSchema(List<String> queries, Schema newSchema) {
    SqlValidator validator = createValidator(newSchema);

    for (String query : queries) {
        try {
            SqlNode sqlNode = parser.parseQuery(query);
            validator.validate(sqlNode);
            System.out.println("OK: " + query);
        } catch (Exception e) {
            System.out.println("FAIL: " + query);
            System.out.println("  Error: " + e.getMessage());
        }
    }
}
```

### Use Case 4: Query Builder Validation

Validate programmatically-constructed queries:

```java
public class QueryBuilder {
    private StringBuilder query = new StringBuilder();
    private SqlValidator validator;

    public QueryBuilder select(String... columns) {
        query.append("SELECT ").append(String.join(", ", columns));
        return this;
    }

    public QueryBuilder from(String table) {
        query.append(" FROM ").append(table);
        return this;
    }

    public String buildAndValidate() throws ValidationException {
        String sql = query.toString();
        try {
            SqlNode sqlNode = parser.parseQuery(sql);
            validator.validate(sqlNode);
            return sql;
        } catch (Exception e) {
            throw new ValidationException("Invalid query: " + e.getMessage());
        }
    }
}
```

## Getting Result Metadata

After successful validation, you can get information about the result:

```java
SqlNode validatedNode = validator.validate(sqlNode);
RelDataType resultType = validator.getValidatedNodeType(validatedNode);

// Get column names and types
for (RelDataTypeField field : resultType.getFieldList()) {
    System.out.println(field.getName() + ": " + field.getType());
}

// Example output:
// EMPLOYEE_ID: INTEGER
// FIRST_NAME: VARCHAR(50)
// SALARY: DECIMAL(10, 2)
```

## Complete Working Example

```java
public class SqlValidationExample {

    public void validateQuery(String sql) {
        try {
            // 1. Parse
            SqlParser parser = SqlParser.create(sql);
            SqlNode sqlNode = parser.parseQuery();

            // 2. Create validator
            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
            CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
            rootSchema.add("COMPANY", new CompanySchema());

            Properties props = new Properties();
            props.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);

            CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList("COMPANY"),
                typeFactory,
                config
            );

            SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT.withIdentifierExpansion(true)
            );

            // 3. Validate
            SqlNode validated = validator.validate(sqlNode);
            System.out.println("Valid! Result: " + validator.getValidatedNodeType(validated));

        } catch (SqlParseException e) {
            System.out.println("Syntax error: " + e.getMessage());
        } catch (CalciteContextException e) {
            System.out.println("Validation error: " + e.getMessage());
        }
    }
}
```

## Running the Examples

```bash
# Custom schema validation
mvn exec:java -Dexec.mainClass="com.example.SqlValidationExample"

# JDBC database validation
mvn exec:java -Dexec.mainClass="com.example.JdbcValidationExample"
```

## Important Notes

1. **Parse Before Validate**: Always parse first, then validate
2. **Exception Types**: `SqlParseException` for syntax errors, `CalciteContextException` for semantic errors
3. **Case Sensitivity**: Use `CASE_SENSITIVE=false` for case-insensitive identifier matching
4. **Schema Path**: The catalog reader's default path determines unqualified table resolution
5. **Calcite 1.37.0**: `CalciteContextException` moved to `org.apache.calcite.runtime` package

## Further Reading

- [Calcite SqlValidator API](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/validate/SqlValidator.html)
- [Calcite SqlParser API](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/parser/SqlParser.html)
- [Calcite Adapter Documentation](https://calcite.apache.org/docs/adapter.html)
