package com.example;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom schema that defines tables and their columns.
 * This represents your database structure.
 */
public class CompanySchema extends AbstractSchema {
    
    @Override
    public Map<String, org.apache.calcite.schema.Table> getTableMap() {
        Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();
        
        // Define EMPLOYEES table
        tables.put("EMPLOYEES", new EmployeesTable());
        
        // Define DEPARTMENTS table
        tables.put("DEPARTMENTS", new DepartmentsTable());
        
        return tables;
    }
    
    /**
     * Table definition for EMPLOYEES
     */
    private static class EmployeesTable extends AbstractTable {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("EMPLOYEE_ID", SqlTypeName.INTEGER)
                    .add("FIRST_NAME", SqlTypeName.VARCHAR, 50)
                    .add("LAST_NAME", SqlTypeName.VARCHAR, 50)
                    .add("EMAIL", SqlTypeName.VARCHAR, 100)
                    .add("DEPARTMENT_ID", SqlTypeName.INTEGER)
                    .add("SALARY", SqlTypeName.DECIMAL, 10, 2)
                    .add("HIRE_DATE", SqlTypeName.DATE)
                    .build();
        }
    }
    
    /**
     * Table definition for DEPARTMENTS
     */
    private static class DepartmentsTable extends AbstractTable {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("DEPARTMENT_ID", SqlTypeName.INTEGER)
                    .add("DEPARTMENT_NAME", SqlTypeName.VARCHAR, 100)
                    .add("LOCATION", SqlTypeName.VARCHAR, 100)
                    .add("MANAGER_ID", SqlTypeName.INTEGER)
                    .build();
        }
    }
}
