# Column Analysis with Apache Calcite

This guide shows how to extract input and output columns from SQL queries using Calcite.

## Overview

When analyzing a SQL query, you typically want to know:
1. **Output Columns**: What columns appear in the result set
2. **Input Columns**: Which table columns are referenced in the query
3. **Column Lineage**: How output columns map back to source columns

## Quick Example

```java
// Parse and validate query
SqlParser parser = SqlParser.create(sql);
SqlNode sqlNode = parser.parseQuery();
SqlValidator validator = createValidator();
SqlNode validatedNode = validator.validate(sqlNode);

// Get OUTPUT columns (result set)
RelDataType resultType = validator.getValidatedNodeType(validatedNode);
for (RelDataTypeField field : resultType.getFieldList()) {
    System.out.println(field.getName() + ": " + field.getType());
}

// Get INPUT columns (source tables)
List<List<String>> fieldOrigins = validator.getFieldOrigins(validatedNode);
for (List<String> origin : fieldOrigins) {
    if (origin != null && origin.size() >= 2) {
        String table = origin.get(origin.size() - 2);
        String column = origin.get(origin.size() - 1);
        System.out.println(table + "." + column);
    }
}
```

## Key Methods

### 1. `getValidatedNodeType()` - Output Columns

Returns the structure of the query result:

```java
RelDataType resultType = validator.getValidatedNodeType(validatedNode);
```

**Returns:** A `RelDataType` containing:
- Column names
- Data types
- Nullability
- Precision/scale

**Example Output:**
```
EMPLOYEE_ID (INTEGER, not null)
FIRST_NAME (VARCHAR(50), nullable)
SALARY (DECIMAL(10,2), nullable)
```

### 2. `getFieldOrigins()` - Input Columns

Returns the source origin of each output column:

```java
List<List<String>> fieldOrigins = validator.getFieldOrigins(validatedNode);
```

**Returns:** A list where each element is `[..., table, column]` (last two elements are table and column)
- `null` entries indicate computed/derived columns
- Each position corresponds to an output column
- The list may have varying length depending on schema depth (e.g., `[schema, table, column]`)

**Example:**
```
Query: SELECT EMPLOYEE_ID, FIRST_NAME, SALARY * 12 as ANNUAL FROM EMPLOYEES

fieldOrigins[0] = ["COMPANY", "EMPLOYEES", "EMPLOYEE_ID"]
fieldOrigins[1] = ["COMPANY", "EMPLOYEES", "FIRST_NAME"]
fieldOrigins[2] = null  // computed expression
```

## Use Cases

### Use Case 1: Query Validation
Verify that all referenced columns exist before executing the query.

```java
List<List<String>> origins = validator.getFieldOrigins(validatedNode);
Set<String> missingColumns = new HashSet<>();

for (List<String> origin : origins) {
    if (origin != null && origin.size() >= 4) {
        String table = origin.get(2);
        String column = origin.get(3);
        // Check if table.column exists in your schema
    }
}
```

### Use Case 2: Data Lineage Tracking
Track where each output column comes from:

```java
RelDataType resultType = validator.getValidatedNodeType(validatedNode);
List<List<String>> origins = validator.getFieldOrigins(validatedNode);

for (int i = 0; i < resultType.getFieldCount(); i++) {
    String outputCol = resultType.getFieldList().get(i).getName();
    List<String> origin = origins.get(i);
    
    if (origin != null) {
        System.out.println(outputCol + " comes from " + 
            origin.get(2) + "." + origin.get(3));
    } else {
        System.out.println(outputCol + " is a computed column");
    }
}
```

### Use Case 3: Column-Level Security
Determine which source columns a query accesses:

```java
List<List<String>> origins = validator.getFieldOrigins(validatedNode);
Set<String> accessedColumns = new HashSet<>();

for (List<String> origin : origins) {
    if (origin != null && origin.size() >= 4) {
        accessedColumns.add(origin.get(2) + "." + origin.get(3));
    }
}

// Check permissions for accessed columns
for (String col : accessedColumns) {
    if (!user.hasAccess(col)) {
        throw new SecurityException("No access to " + col);
    }
}
```

### Use Case 4: Query Optimization Hints
Understand column usage patterns:

```java
// Identify which columns are actually used from large tables
List<List<String>> origins = validator.getFieldOrigins(validatedNode);

Map<String, Set<String>> tableColumns = new HashMap<>();
for (List<String> origin : origins) {
    if (origin != null && origin.size() >= 4) {
        String table = origin.get(2);
        String column = origin.get(3);
        tableColumns.computeIfAbsent(table, k -> new HashSet<>()).add(column);
    }
}

// Suggest indexes or column pruning
System.out.println("Columns used by table:");
tableColumns.forEach((table, cols) -> 
    System.out.println(table + ": " + cols));
```

## Example Queries

### Simple SELECT
```sql
SELECT EMPLOYEE_ID, FIRST_NAME FROM EMPLOYEES
```
- Output: `EMPLOYEE_ID`, `FIRST_NAME`
- Input: `EMPLOYEES.EMPLOYEE_ID`, `EMPLOYEES.FIRST_NAME`

### JOIN
```sql
SELECT E.FIRST_NAME, D.DEPARTMENT_NAME 
FROM EMPLOYEES E 
JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.DEPARTMENT_ID
```
- Output: `FIRST_NAME`, `DEPARTMENT_NAME`
- Input: `EMPLOYEES.FIRST_NAME`, `EMPLOYEES.DEPARTMENT_ID`, `DEPARTMENTS.DEPARTMENT_ID`, `DEPARTMENTS.DEPARTMENT_NAME`

### Computed Columns
```sql
SELECT FIRST_NAME || ' ' || LAST_NAME as FULL_NAME,
       SALARY * 12 as ANNUAL_SALARY
FROM EMPLOYEES
```
- Output: `FULL_NAME`, `ANNUAL_SALARY`
- Input: `EMPLOYEES.FIRST_NAME`, `EMPLOYEES.LAST_NAME`, `EMPLOYEES.SALARY`
- Lineage: 
  - `FULL_NAME` ← derived from `FIRST_NAME` and `LAST_NAME`
  - `ANNUAL_SALARY` ← derived from `SALARY`

### Aggregates
```sql
SELECT DEPARTMENT_ID, COUNT(*) as EMP_COUNT
FROM EMPLOYEES
GROUP BY DEPARTMENT_ID
```
- Output: `DEPARTMENT_ID`, `EMP_COUNT`
- Input: `EMPLOYEES.DEPARTMENT_ID`, `EMPLOYEES.*` (for COUNT)

## Important Notes

1. **Validation Required**: You must validate the query before calling `getFieldOrigins()`
2. **Null Origins**: Computed/derived columns return `null` in `fieldOrigins`
3. **Complex Expressions**: For complex expressions, you may need to analyze the relational algebra tree
4. **Wildcards**: `SELECT *` expands to all columns, each tracked individually

## Running the Examples

```bash
# Simple column extraction
mvn exec:java -Dexec.mainClass="com.example.SimpleColumnExample"

# Advanced column analysis
mvn exec:java -Dexec.mainClass="com.example.ColumnAnalysisExample"
```

## Further Reading

- [Calcite SqlValidator API](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/validate/SqlValidator.html)
- [RelDataType Documentation](https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/type/RelDataType.html)
- [Column Lineage in Data Systems](https://en.wikipedia.org/wiki/Data_lineage)
