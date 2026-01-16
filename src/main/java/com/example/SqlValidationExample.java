package com.example;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;

import java.util.Collections;
import java.util.Properties;

/**
 * Example demonstrating SQL validation against a data source using Apache Calcite.
 */
public class SqlValidationExample {

    public static void main(String[] args) {
        SqlValidationExample example = new SqlValidationExample();
        
        System.out.println("=== Apache Calcite SQL Validation Example ===\n");
        
        // Test various SQL queries
        String[] testQueries = {
            // Valid queries
            "SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME FROM EMPLOYEES",
            "SELECT E.FIRST_NAME, D.DEPARTMENT_NAME FROM EMPLOYEES E JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.DEPARTMENT_ID",
            "SELECT DEPARTMENT_ID, COUNT(*) as EMP_COUNT FROM EMPLOYEES GROUP BY DEPARTMENT_ID",
            
            // Invalid queries (will fail validation)
            "SELECT INVALID_COLUMN FROM EMPLOYEES",
            "SELECT * FROM NON_EXISTENT_TABLE",
            "SELECT FIRST_NAME FROM EMPLOYEES WHERE INVALID_COLUMN = 'test'"
        };
        
        for (String query : testQueries) {
            example.validateQuery(query);
        }
    }
    
    public void validateQuery(String sql) {
        System.out.println("Query: " + sql);
        System.out.println("----------------------------------------");
        
        try {
            // Step 1: Parse the SQL
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode sqlNode = parser.parseQuery();
            System.out.println("✓ Parsing successful");
            
            // Step 2: Create the validator with schema information
            SqlValidator validator = createValidator();
            
            // Step 3: Validate the SQL against the schema
            SqlNode validatedNode = validator.validate(sqlNode);
            System.out.println("✓ Validation successful");
            
            // Step 4: Show the validated query with inferred types
            System.out.println("Validated SQL: " + validatedNode.toString());
            System.out.println("Result type: " + validator.getValidatedNodeType(validatedNode));
            
        } catch (SqlParseException e) {
            System.out.println("✗ Parse error: " + e.getMessage());
        } catch (CalciteContextException e) {
            System.out.println("✗ Validation error: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("✗ Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println();
    }
    
    /**
     * Creates a SqlValidator configured with our custom schema.
     */
    private SqlValidator createValidator() {
        // Create type factory
        RelDataTypeFactory typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        
        // Create root schema and add our company schema
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("COMPANY", new CompanySchema());
        
        // Create connection config
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);
        
        // Create catalog reader - this provides table and column metadata
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList("COMPANY"), // Default schema path
            typeFactory,
            config
        );
        
        // Create validator configuration
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
            .withIdentifierExpansion(true)
            .withSqlConformance(SqlConformanceEnum.DEFAULT);
        
        // Create and return the validator
        return SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            validatorConfig
        );
    }
}
