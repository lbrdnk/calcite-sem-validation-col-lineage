#!/bin/bash

cat << 'EOF'
====================================================================
Apache Calcite Column Analysis Example - Code Walkthrough
====================================================================

This example shows how to extract column lineage from SQL queries:
- OUTPUT COLUMNS: What the query returns (result set structure)
- INPUT COLUMNS: What source columns are referenced
- COLUMN LINEAGE: Full expression tree mapping outputs to inputs

====================================================================
CORE CONCEPTS
====================================================================

1. OUTPUT COLUMNS
-----------------
The columns that appear in the query's result set.

Query: SELECT FIRST_NAME, SALARY * 12 as ANNUAL FROM EMPLOYEES
Output columns: FIRST_NAME, ANNUAL

2. INPUT COLUMNS
-----------------
All source table columns referenced anywhere in the query
(SELECT list, WHERE clause, JOIN conditions, GROUP BY, etc.)

Query: SELECT FIRST_NAME FROM EMPLOYEES WHERE DEPARTMENT_ID = 10
Input columns: EMPLOYEES.FIRST_NAME, EMPLOYEES.DEPARTMENT_ID

3. COLUMN LINEAGE
-----------------
The full expression tree showing how each output column is derived
from source columns through operations.

Output: ANNUAL
Lineage: *(EMPLOYEES.SALARY, 12)
         └── [*]
             ├── [Column] EMPLOYEES.SALARY
             └── [Literal] 12

====================================================================
STEP-BY-STEP CODE EXPLANATION
====================================================================

STEP 1: Parse and Validate the SQL
-----------------------------------
```java
// Parse SQL string into AST
SqlParser parser = SqlParser.create(sql);
SqlNode sqlNode = parser.parseQuery();

// Validate against schema (ensures tables/columns exist)
SqlValidator validator = createValidator();
SqlNode validatedNode = validator.validate(sqlNode);
```

STEP 2: Get Output Columns (Result Set Structure)
--------------------------------------------------
```java
// Get the validated result type - this is the output schema
RelDataType resultType = validator.getValidatedNodeType(validatedNode);

for (RelDataTypeField field : resultType.getFieldList()) {
    System.out.println(field.getName() + ": " + field.getType());
}
// Output: FIRST_NAME: VARCHAR(50)
//         ANNUAL: DECIMAL(12, 2)
```

STEP 3: Convert to Relational Algebra (RelNode)
------------------------------------------------
To extract full lineage, we need the relational algebra representation:

```java
// Create converter
SqlToRelConverter converter = new SqlToRelConverter(
    null,
    validator,
    catalogReader,
    cluster,
    StandardConvertletTable.INSTANCE,
    config
);

// Convert validated SQL to relational algebra tree
RelRoot root = converter.convertQuery(validatedNode, false, true);
RelNode relNode = root.rel;
```

The RelNode tree looks like:
```
Project [FIRST_NAME, SALARY * 12 as ANNUAL]
  └── Filter [DEPARTMENT_ID = 10]
        └── TableScan [EMPLOYEES]
```

STEP 4: Extract Input Columns from RelNode Tree
------------------------------------------------
Walk the RelNode tree to find all column references:

```java
private void extractColumnsRecursive(RelNode relNode, Set<String> columns) {
    // For Project nodes, extract columns from expressions
    if (relNode instanceof Project) {
        Project project = (Project) relNode;
        for (RexNode expr : project.getProjects()) {
            extractColumnsFromRex(expr, columns, project.getInput());
        }
    }

    // For Filter nodes, extract columns from condition
    if (relNode instanceof Filter) {
        Filter filter = (Filter) relNode;
        extractColumnsFromRex(filter.getCondition(), columns, filter.getInput());
    }

    // For Join nodes, extract columns from join condition
    if (relNode instanceof Join) {
        Join join = (Join) relNode;
        extractColumnsFromRex(join.getCondition(), columns, join);
    }

    // Recursively process child nodes
    for (RelNode input : relNode.getInputs()) {
        extractColumnsRecursive(input, columns);
    }
}
```

STEP 5: Build Full Expression Lineage
--------------------------------------
For each output column, build the complete expression tree:

```java
private ExpressionNode buildExpressionTree(RexNode rex, RelNode sourceNode) {
    if (rex instanceof RexInputRef) {
        // Column reference - resolve to table.column
        RexInputRef ref = (RexInputRef) rex;
        return resolveToColumn(ref.getIndex(), sourceNode);
    }
    else if (rex instanceof RexLiteral) {
        // Literal value
        return ExpressionNode.literal(rex.toString());
    }
    else if (rex instanceof RexCall) {
        // Operation (function call, operator)
        RexCall call = (RexCall) rex;
        String operator = call.getOperator().getName();

        // Recursively build children
        List<ExpressionNode> children = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            children.add(buildExpressionTree(operand, sourceNode));
        }

        return ExpressionNode.operation(operator, children);
    }
    return ExpressionNode.unknown();
}
```

STEP 6: Resolve Column Index to Table.Column
---------------------------------------------
Column references in RelNode are indices that need to be resolved:

```java
private String resolveColumnFromIndex(RelNode node, int index) {
    if (node instanceof TableScan) {
        // Direct table reference
        TableScan scan = (TableScan) node;
        String tableName = scan.getTable().getQualifiedName().get(...);
        String colName = scan.getRowType().getFieldList().get(index).getName();
        return tableName + "." + colName;
    }
    else if (node instanceof Join) {
        // Join combines columns from left and right
        Join join = (Join) node;
        int leftCount = join.getLeft().getRowType().getFieldCount();
        if (index < leftCount) {
            return resolveColumnFromIndex(join.getLeft(), index);
        } else {
            return resolveColumnFromIndex(join.getRight(), index - leftCount);
        }
    }
    // ... handle other node types
}
```

====================================================================
EXAMPLE OUTPUTS
====================================================================

EXAMPLE 1: Simple SELECT
-------------------------
Query: SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME FROM EMPLOYEES

Output Columns:
  EMPLOYEE_ID    INTEGER
  FIRST_NAME     VARCHAR(50)
  LAST_NAME      VARCHAR(50)

Input Columns:
  EMPLOYEES.EMPLOYEE_ID
  EMPLOYEES.FIRST_NAME
  EMPLOYEES.LAST_NAME

Column Lineage:
  EMPLOYEE_ID:
    Expression: EMPLOYEES.EMPLOYEE_ID
    Source columns: [EMPLOYEES.EMPLOYEE_ID]

  FIRST_NAME:
    Expression: EMPLOYEES.FIRST_NAME
    Source columns: [EMPLOYEES.FIRST_NAME]

  LAST_NAME:
    Expression: EMPLOYEES.LAST_NAME
    Source columns: [EMPLOYEES.LAST_NAME]


EXAMPLE 2: JOIN Query
----------------------
Query: SELECT E.FIRST_NAME, D.DEPARTMENT_NAME
       FROM EMPLOYEES E
       JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.DEPARTMENT_ID

Output Columns:
  FIRST_NAME       VARCHAR(50)
  DEPARTMENT_NAME  VARCHAR(100)

Input Columns:
  EMPLOYEES.FIRST_NAME
  EMPLOYEES.DEPARTMENT_ID      <- from JOIN condition
  DEPARTMENTS.DEPARTMENT_ID    <- from JOIN condition
  DEPARTMENTS.DEPARTMENT_NAME

Column Lineage:
  FIRST_NAME:
    Expression: EMPLOYEES.FIRST_NAME
    Source columns: [EMPLOYEES.FIRST_NAME]

  DEPARTMENT_NAME:
    Expression: DEPARTMENTS.DEPARTMENT_NAME
    Source columns: [DEPARTMENTS.DEPARTMENT_NAME]


EXAMPLE 3: Aggregation
-----------------------
Query: SELECT DEPARTMENT_ID, COUNT(*) as EMP_COUNT, AVG(SALARY) as AVG_SALARY
       FROM EMPLOYEES
       GROUP BY DEPARTMENT_ID

Output Columns:
  DEPARTMENT_ID  INTEGER
  EMP_COUNT      BIGINT
  AVG_SALARY     DECIMAL(10,2)

Input Columns:
  EMPLOYEES.DEPARTMENT_ID
  EMPLOYEES.SALARY

Column Lineage:
  DEPARTMENT_ID:
    Expression: EMPLOYEES.DEPARTMENT_ID
    Source columns: [EMPLOYEES.DEPARTMENT_ID]

  EMP_COUNT:
    Expression: COUNT(*)
    Source columns: []    <- COUNT(*) doesn't reference specific columns

  AVG_SALARY:
    Expression: AVG(EMPLOYEES.SALARY)
    Source columns: [EMPLOYEES.SALARY]


EXAMPLE 4: Computed Expressions
--------------------------------
Query: SELECT FIRST_NAME || ' ' || LAST_NAME as FULL_NAME,
              SALARY * 1.1 as NEW_SALARY
       FROM EMPLOYEES
       WHERE DEPARTMENT_ID = 10

Output Columns:
  FULL_NAME   VARCHAR(101)
  NEW_SALARY  DECIMAL(12,3)

Input Columns:
  EMPLOYEES.FIRST_NAME      <- from FULL_NAME expression
  EMPLOYEES.LAST_NAME       <- from FULL_NAME expression
  EMPLOYEES.SALARY          <- from NEW_SALARY expression
  EMPLOYEES.DEPARTMENT_ID   <- from WHERE clause

Column Lineage:
  FULL_NAME:
    Expression: ||(||(EMPLOYEES.FIRST_NAME, ' '), EMPLOYEES.LAST_NAME)
    Source columns: [EMPLOYEES.FIRST_NAME, EMPLOYEES.LAST_NAME]

  NEW_SALARY:
    Expression: *(EMPLOYEES.SALARY, 1.1)
    Source columns: [EMPLOYEES.SALARY]


====================================================================
KEY CLASSES AND THEIR ROLES
====================================================================

RelNode Hierarchy:
------------------
RelNode (base)
  ├── Project      - SELECT list expressions
  ├── Filter       - WHERE clause
  ├── Join         - JOIN operations
  ├── Aggregate    - GROUP BY + aggregate functions
  └── TableScan    - FROM table reference

RexNode Hierarchy (expressions):
---------------------------------
RexNode (base)
  ├── RexInputRef  - Column reference (by index)
  ├── RexLiteral   - Constant value
  └── RexCall      - Function/operator call with operands


====================================================================
USE CASES FOR COLUMN LINEAGE
====================================================================

1. DATA LINEAGE TRACKING
   Track where each report column comes from in source systems.
   Essential for data governance and compliance.

2. COLUMN-LEVEL SECURITY
   Determine which source columns a query accesses to enforce
   fine-grained access control.

   ```java
   Set<String> accessedColumns = getInputColumns(query);
   for (String col : accessedColumns) {
       if (!user.hasAccess(col)) {
           throw new SecurityException("Access denied to " + col);
       }
   }
   ```

3. IMPACT ANALYSIS
   Before changing a column, find all queries that depend on it.

4. QUERY OPTIMIZATION
   Identify which columns are actually used for column pruning
   and predicate pushdown.

5. DOCUMENTATION GENERATION
   Auto-generate documentation showing data flow in reports/dashboards.

6. DATA QUALITY
   Trace data quality issues back to their source columns.


====================================================================
RUNNING THE EXAMPLE
====================================================================

Build and run:
  mvn clean compile
  mvn exec:java -Dexec.mainClass="com.example.ColumnAnalysisExample"

For simpler output, run:
  mvn exec:java -Dexec.mainClass="com.example.SimpleColumnExample"


====================================================================
RELATED FILES
====================================================================

- ColumnAnalysisExample.java  - Full lineage extraction with expression trees
- SimpleColumnExample.java    - Simpler version showing basic column extraction
- CompanySchema.java          - Sample schema (EMPLOYEES, DEPARTMENTS tables)
- COLUMN_ANALYSIS.md          - API documentation and quick reference

====================================================================
EOF
