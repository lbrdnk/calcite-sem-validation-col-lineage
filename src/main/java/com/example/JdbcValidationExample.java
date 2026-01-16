package com.example;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.Frameworks;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

/**
 * Example showing SQL validation against an actual JDBC database.
 * This example uses H2 in-memory database but works with any JDBC source.
 */
public class JdbcValidationExample {

    public static void main(String[] args) throws SQLException {
        JdbcValidationExample example = new JdbcValidationExample();
        
        // Create and populate a sample H2 database
        DataSource dataSource = example.createSampleDatabase();
        
        System.out.println("=== SQL Validation Against JDBC Database ===\n");
        
        String[] testQueries = {
            // Valid queries
            "SELECT * FROM CUSTOMERS",
            "SELECT customer_id, name, email FROM CUSTOMERS WHERE country = 'USA'",
            "SELECT C.name, O.order_date FROM CUSTOMERS C JOIN ORDERS O ON C.customer_id = O.customer_id",
            
            // Invalid queries
            "SELECT invalid_column FROM CUSTOMERS",
            "SELECT * FROM NON_EXISTENT_TABLE",
            "SELECT name FROM CUSTOMERS WHERE invalid_field = 1",
            "SELECT name - 10 FROM CUSTOMERS"
        };
        
        for (String query : testQueries) {
            example.validateQuery(query, dataSource);
        }
    }
    
    /**
     * Create a sample H2 database with test data
     */
    private DataSource createSampleDatabase() throws SQLException {
        // Create H2 in-memory database
        String jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
        Connection conn = DriverManager.getConnection(jdbcUrl, "sa", "");
        
        Statement stmt = conn.createStatement();
        
        // Create CUSTOMERS table
        stmt.execute(
            "CREATE TABLE CUSTOMERS (" +
            "  customer_id INT PRIMARY KEY," +
            "  name VARCHAR(100)," +
            "  email VARCHAR(100)," +
            "  country VARCHAR(50)" +
            ")"
        );
        
        // Create ORDERS table
        stmt.execute(
            "CREATE TABLE ORDERS (" +
            "  order_id INT PRIMARY KEY," +
            "  customer_id INT," +
            "  order_date DATE," +
            "  total_amount DECIMAL(10,2)," +
            "  FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)" +
            ")"
        );
        
        // Insert sample data
        stmt.execute("INSERT INTO CUSTOMERS VALUES (1, 'John Doe', 'john@example.com', 'USA')");
        stmt.execute("INSERT INTO CUSTOMERS VALUES (2, 'Jane Smith', 'jane@example.com', 'UK')");
        stmt.execute("INSERT INTO ORDERS VALUES (1, 1, '2024-01-15', 150.00)");
        stmt.execute("INSERT INTO ORDERS VALUES (2, 2, '2024-01-20', 200.00)");
        
        System.out.println("Sample database created with CUSTOMERS and ORDERS tables\n");
        
        // Keep connection open by not closing it
        // Return a simple DataSource implementation
        final String url = jdbcUrl;
        return new javax.sql.DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return DriverManager.getConnection(url, "sa", "");
            }
            
            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                return DriverManager.getConnection(url, username, password);
            }
            
            @Override
            public java.io.PrintWriter getLogWriter() { return null; }
            
            @Override
            public void setLogWriter(java.io.PrintWriter out) {}
            
            @Override
            public void setLoginTimeout(int seconds) {}
            
            @Override
            public int getLoginTimeout() { return 0; }
            
            @Override
            public java.util.logging.Logger getParentLogger() { return null; }
            
            @Override
            public <T> T unwrap(Class<T> iface) { return null; }
            
            @Override
            public boolean isWrapperFor(Class<?> iface) { return false; }
        };
    }
    
    /**
     * Validate SQL query against the JDBC database schema
     */
    public void validateQuery(String sql, DataSource dataSource) {
        System.out.println("Query: " + sql);
        System.out.println("----------------------------------------");
        
        try {
            // Step 1: Parse the SQL
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode sqlNode = parser.parseQuery();
            System.out.println("✓ Parsing successful");
            
            // Step 2: Create validator with JDBC schema
            SqlValidator validator = createJdbcValidator(dataSource);
            
            // Step 3: Validate against actual database schema
            SqlNode validatedNode = validator.validate(sqlNode);
            System.out.println("✓ Validation successful");
            System.out.println("✓ All tables and columns exist in the database");
            
            // Show result type
            System.out.println("Result type: " + validator.getValidatedNodeType(validatedNode));
            
        } catch (SqlParseException e) {
            System.out.println("✗ Parse error: " + e.getMessage());
        } catch (CalciteContextException e) {
            System.out.println("✗ Validation error: " + e.getMessage());
            // Extract the specific validation error
            if (e.getCause() != null) {
                System.out.println("  Reason: " + e.getCause().getMessage());
            }
        } catch (Exception e) {
            System.out.println("✗ Error: " + e.getMessage());
        }
        
        System.out.println();
    }
    
    /**
     * Creates a SqlValidator that reads schema metadata from a JDBC database
     */
    private SqlValidator createJdbcValidator(DataSource dataSource) {
        // Create type factory
        RelDataTypeFactory typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        
        // Create root schema first
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        
        // Create JDBC schema that will read metadata from the database
        JdbcSchema jdbcSchema = JdbcSchema.create(
            rootSchema.plus(),
            "PUBLIC",
            dataSource,
            null,  // catalog
            null   // schema - will read all schemas
        );
        
        // Add JDBC schema to root
        rootSchema.add("PUBLIC", jdbcSchema);  // H2 default schema is PUBLIC
        
        // Create connection config
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);
        
        // Create catalog reader - this will query the JDBC database for metadata
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList("PUBLIC"),
            typeFactory,
            config
        );
        
        // Create validator configuration with strict type checking
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
            .withIdentifierExpansion(true)
            .withSqlConformance(SqlConformanceEnum.DEFAULT)
            .withTypeCoercionEnabled(false);  // Disable implicit type coercion
        
        // Create and return validator
        return SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            validatorConfig
        );
    }
}
