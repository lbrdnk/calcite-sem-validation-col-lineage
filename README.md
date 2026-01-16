# Apache Calcite SQL Validation Examples

This project demonstrates how to use Apache Calcite to validate SQL queries against data sources, ensuring that referenced tables and columns actually exist.

It also covers example of column lineage tracking.

## Overview

The project contains three main examples:

### 1. SqlValidationExample.java
Validates SQL against a **custom schema definition**. This approach is useful when:
- You want to define your schema programmatically
- You're building a query engine without an existing database
- You need full control over table and column definitions

**Key Features:**
- Custom schema with EMPLOYEES and DEPARTMENTS tables
- Validates column names, table references, and data types
- Shows how to define tables using `RelDataTypeFactory`

### 2. JdbcValidationExample.java
Validates SQL against a **real JDBC database**. This approach is useful when:
- You have an existing database (PostgreSQL, MySQL, Oracle, etc.)
- You want to validate queries before execution
- You need to work with actual database metadata

**Key Features:**
- Creates an H2 in-memory database with sample data
- Uses `JdbcSchema` to automatically read table metadata from the database
- Validates queries against actual database structure

### 3. ColumnAnalysisExample.java
Advanced **column lineage analysis**:
- Full output-to-input column mapping
- Identifies all tables referenced in a query
- Handles computed/derived columns
- Useful for data lineage tracking and column-level security

## Building and Running

### Prerequisites
- Java 11 or higher
- Maven

### Build the project:
```bash
mvn clean compile
```

### Run the examples:
```bash
# Custom schema validation
mvn exec:java -Dexec.mainClass="com.example.SqlValidationExample"

# JDBC-based validation
mvn exec:java -Dexec.mainClass="com.example.JdbcValidationExample"

# Advanced column lineage analysis
mvn exec:java -Dexec.mainClass="com.example.ColumnAnalysisExample"
```
