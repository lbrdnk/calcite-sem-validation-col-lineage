package com.example;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Example showing how to extract input columns (from source tables)
 * and output columns (in result set) from a SQL query.
 */
public class ColumnAnalysisExample {

    /**
     * Represents the lineage of a single output column.
     * Contains the expression tree showing all operations and source columns.
     */
    public static class ColumnLineage {
        private final String outputColumn;
        private final ExpressionNode expression;
        private final Set<String> sourceColumns;

        public ColumnLineage(String outputColumn, ExpressionNode expression, Set<String> sourceColumns) {
            this.outputColumn = outputColumn;
            this.expression = expression;
            this.sourceColumns = sourceColumns;
        }

        public String getOutputColumn() { return outputColumn; }
        public ExpressionNode getExpression() { return expression; }
        public Set<String> getSourceColumns() { return sourceColumns; }

        @Override
        public String toString() {
            return outputColumn + " <- " + expression.toString();
        }
    }

    /**
     * Represents a node in an expression tree.
     * Can be a column reference, literal, or operation with child nodes.
     */
    public static class ExpressionNode {
        public enum NodeType { COLUMN, LITERAL, OPERATION }

        private final NodeType type;
        private final String value;           // Column name, literal value, or operator name
        private final List<ExpressionNode> children;
        private final String tableName;       // For COLUMN type

        // Column reference
        public static ExpressionNode column(String tableName, String columnName) {
            return new ExpressionNode(NodeType.COLUMN, columnName, null, tableName);
        }

        // Literal value
        public static ExpressionNode literal(String value) {
            return new ExpressionNode(NodeType.LITERAL, value, null, null);
        }

        // Operation with children
        public static ExpressionNode operation(String operator, List<ExpressionNode> children) {
            return new ExpressionNode(NodeType.OPERATION, operator, children, null);
        }

        private ExpressionNode(NodeType type, String value, List<ExpressionNode> children, String tableName) {
            this.type = type;
            this.value = value;
            this.children = children != null ? children : Collections.emptyList();
            this.tableName = tableName;
        }

        public NodeType getType() { return type; }
        public String getValue() { return value; }
        public List<ExpressionNode> getChildren() { return children; }
        public String getTableName() { return tableName; }

        public String getFullColumnName() {
            if (type == NodeType.COLUMN && tableName != null) {
                return tableName + "." + value;
            }
            return value;
        }

        /**
         * Collect all source columns from this expression tree
         */
        public void collectSourceColumns(Set<String> columns) {
            if (type == NodeType.COLUMN) {
                columns.add(getFullColumnName());
            }
            for (ExpressionNode child : children) {
                child.collectSourceColumns(columns);
            }
        }

        @Override
        public String toString() {
            return format(0);
        }

        public String format(int indent) {
            String pad = "  ".repeat(indent);
            StringBuilder sb = new StringBuilder();

            switch (type) {
                case COLUMN:
                    sb.append(getFullColumnName());
                    break;
                case LITERAL:
                    sb.append(value);
                    break;
                case OPERATION:
                    sb.append(value).append("(");
                    if (children.size() <= 2 && children.stream().allMatch(c -> c.type != NodeType.OPERATION)) {
                        // Simple expression - inline
                        sb.append(children.stream()
                            .map(ExpressionNode::toString)
                            .collect(Collectors.joining(", ")));
                    } else {
                        // Complex expression - multi-line
                        sb.append("\n");
                        for (int i = 0; i < children.size(); i++) {
                            sb.append(pad).append("  ").append(children.get(i).format(indent + 1));
                            if (i < children.size() - 1) sb.append(",");
                            sb.append("\n");
                        }
                        sb.append(pad);
                    }
                    sb.append(")");
                    break;
            }
            return sb.toString();
        }

        /**
         * Format as a tree structure for display
         */
        public String toTreeString(String prefix, boolean isLast) {
            StringBuilder sb = new StringBuilder();
            sb.append(prefix);
            sb.append(isLast ? "└── " : "├── ");

            switch (type) {
                case COLUMN:
                    sb.append("[Column] ").append(getFullColumnName());
                    break;
                case LITERAL:
                    sb.append("[Literal] ").append(value);
                    break;
                case OPERATION:
                    sb.append("[").append(value).append("]");
                    break;
            }
            sb.append("\n");

            String childPrefix = prefix + (isLast ? "    " : "│   ");
            for (int i = 0; i < children.size(); i++) {
                sb.append(children.get(i).toTreeString(childPrefix, i == children.size() - 1));
            }
            return sb.toString();
        }
    }

    public static void main(String[] args) {
        ColumnAnalysisExample example = new ColumnAnalysisExample();
        
        System.out.println("=== SQL Column Analysis Example ===\n");
        
        String[] testQueries = {
            "SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME FROM EMPLOYEES",
            
            "SELECT E.FIRST_NAME, E.LAST_NAME, D.DEPARTMENT_NAME " +
            "FROM EMPLOYEES E JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.DEPARTMENT_ID",
            
            "SELECT DEPARTMENT_ID, COUNT(*) as EMP_COUNT, AVG(SALARY) as AVG_SALARY " +
            "FROM EMPLOYEES " +
            "GROUP BY DEPARTMENT_ID",
            
            "SELECT FIRST_NAME || ' ' || LAST_NAME as FULL_NAME, " +
            "SALARY * 1.1 as NEW_SALARY " +
            "FROM EMPLOYEES " +
            "WHERE DEPARTMENT_ID = 10"
        };
        
        for (String query : testQueries) {
            example.analyzeQuery(query);
        }
    }
    
    public void analyzeQuery(String sql) {
        System.out.println("Query: " + sql);
        System.out.println("=".repeat(80));
        
        try {
            // Step 1: Parse SQL
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode sqlNode = parser.parseQuery();
            
            // Step 2: Validate
            SqlValidator validator = createValidator();
            SqlNode validatedNode = validator.validate(sqlNode);
            
            // Step 3: Get output columns (result set columns)
            RelDataType resultType = validator.getValidatedNodeType(validatedNode);
            System.out.println("\n📤 OUTPUT COLUMNS (Result Set):");
            System.out.println("   " + "-".repeat(70));
            for (RelDataTypeField field : resultType.getFieldList()) {
                System.out.println(String.format("   %-30s %s", 
                    field.getName(), 
                    field.getType().toString()));
            }
            
            // Step 4: Get input columns (source table columns)
            // This requires converting to relational algebra
            RelNode relNode = convertToRelNode(validatedNode, validator);
            
            System.out.println("\n📥 INPUT COLUMNS (Source Tables):");
            System.out.println("   " + "-".repeat(70));

            // Extract ALL input columns from the RelNode tree (includes expressions, WHERE, etc.)
            Set<String> allInputColumns = extractAllInputColumns(relNode);
            for (String input : allInputColumns.stream().sorted().collect(Collectors.toList())) {
                System.out.println("   " + input);
            }

            List<List<String>> inputColumns = validator.getFieldOrigins(validatedNode);
            
            // Step 5: Show full column lineage (output -> expression tree -> input columns)
            System.out.println("\n🔗 COLUMN LINEAGE (Output -> Expression -> Source Columns):");
            System.out.println("   " + "-".repeat(70));

            List<ColumnLineage> lineages = buildColumnLineage(relNode, resultType);
            for (ColumnLineage lineage : lineages) {
                System.out.println("\n   " + lineage.getOutputColumn() + ":");
                System.out.println("      Expression: " + lineage.getExpression().toString());
                System.out.println("      Source columns: " + lineage.getSourceColumns());
            }
            
            // Step 6: Additional analysis - tables used
            System.out.println("\n📊 TABLES REFERENCED:");
            System.out.println("   " + "-".repeat(70));
            Set<String> tables = extractTablesUsed(relNode);
            for (String table : tables.stream().sorted().collect(Collectors.toList())) {
                System.out.println("   " + table);
            }
            
        } catch (Exception e) {
            System.out.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("\n");
    }
    
    /**
     * Build full column lineage for all output columns
     */
    private List<ColumnLineage> buildColumnLineage(RelNode relNode, RelDataType resultType) {
        List<ColumnLineage> lineages = new ArrayList<>();
        List<RelDataTypeField> outputFields = resultType.getFieldList();

        // Find the top-level Project node
        RelNode current = relNode;
        while (current != null && !(current instanceof Project) && !(current instanceof Aggregate)) {
            if (current.getInputs().isEmpty()) break;
            current = current.getInput(0);
        }

        if (current instanceof Project) {
            Project project = (Project) current;
            List<RexNode> projects = project.getProjects();

            for (int i = 0; i < outputFields.size() && i < projects.size(); i++) {
                String outputName = outputFields.get(i).getName();
                RexNode expr = projects.get(i);

                ExpressionNode exprNode = buildExpressionTree(expr, project.getInput());
                Set<String> sourceColumns = new LinkedHashSet<>();
                exprNode.collectSourceColumns(sourceColumns);

                lineages.add(new ColumnLineage(outputName, exprNode, sourceColumns));
            }
        } else if (current instanceof Aggregate) {
            Aggregate aggregate = (Aggregate) current;
            RelNode input = aggregate.getInput();

            // Group by columns
            int groupCount = aggregate.getGroupCount();
            for (int i = 0; i < groupCount; i++) {
                int groupIdx = aggregate.getGroupSet().nth(i);
                String outputName = outputFields.get(i).getName();

                ExpressionNode exprNode = buildExpressionTreeFromIndex(groupIdx, input);
                Set<String> sourceColumns = new LinkedHashSet<>();
                exprNode.collectSourceColumns(sourceColumns);

                lineages.add(new ColumnLineage(outputName, exprNode, sourceColumns));
            }

            // Aggregate calls
            List<AggregateCall> aggCalls = aggregate.getAggCallList();
            for (int i = 0; i < aggCalls.size(); i++) {
                int outputIdx = groupCount + i;
                if (outputIdx >= outputFields.size()) break;

                String outputName = outputFields.get(outputIdx).getName();
                AggregateCall aggCall = aggCalls.get(i);

                ExpressionNode exprNode = buildAggregateExpression(aggCall, input);
                Set<String> sourceColumns = new LinkedHashSet<>();
                exprNode.collectSourceColumns(sourceColumns);

                lineages.add(new ColumnLineage(outputName, exprNode, sourceColumns));
            }
        } else {
            // Fallback: just list direct columns
            for (int i = 0; i < outputFields.size(); i++) {
                String outputName = outputFields.get(i).getName();
                ExpressionNode exprNode = buildExpressionTreeFromIndex(i, relNode);
                Set<String> sourceColumns = new LinkedHashSet<>();
                exprNode.collectSourceColumns(sourceColumns);
                lineages.add(new ColumnLineage(outputName, exprNode, sourceColumns));
            }
        }

        return lineages;
    }

    /**
     * Build an expression tree from a RexNode
     */
    private ExpressionNode buildExpressionTree(RexNode rex, RelNode sourceNode) {
        if (rex instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) rex;
            return buildExpressionTreeFromIndex(ref.getIndex(), sourceNode);
        } else if (rex instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rex;
            String value = literal.getValue2() != null ? literal.getValue2().toString() : "NULL";
            // Add quotes for string literals
            if (literal.getTypeName().getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER) {
                value = "'" + value + "'";
            }
            return ExpressionNode.literal(value);
        } else if (rex instanceof RexCall) {
            RexCall call = (RexCall) rex;
            String operator = call.getOperator().getName();

            List<ExpressionNode> children = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                children.add(buildExpressionTree(operand, sourceNode));
            }

            return ExpressionNode.operation(operator, children);
        }

        return ExpressionNode.literal("?");
    }

    /**
     * Build an expression tree from a column index
     */
    private ExpressionNode buildExpressionTreeFromIndex(int index, RelNode node) {
        if (node instanceof TableScan) {
            TableScan scan = (TableScan) node;
            String tableName = scan.getTable().getQualifiedName()
                .get(scan.getTable().getQualifiedName().size() - 1);
            RelDataType rowType = scan.getRowType();
            if (index < rowType.getFieldCount()) {
                String colName = rowType.getFieldList().get(index).getName();
                return ExpressionNode.column(tableName, colName);
            }
        } else if (node instanceof Join) {
            Join join = (Join) node;
            int leftFieldCount = join.getLeft().getRowType().getFieldCount();
            if (index < leftFieldCount) {
                return buildExpressionTreeFromIndex(index, join.getLeft());
            } else {
                return buildExpressionTreeFromIndex(index - leftFieldCount, join.getRight());
            }
        } else if (node instanceof Project) {
            Project project = (Project) node;
            if (index < project.getProjects().size()) {
                RexNode expr = project.getProjects().get(index);
                return buildExpressionTree(expr, project.getInput());
            }
        } else if (node instanceof Filter) {
            return buildExpressionTreeFromIndex(index, node.getInput(0));
        } else if (node.getInputs().size() == 1) {
            return buildExpressionTreeFromIndex(index, node.getInput(0));
        }

        return ExpressionNode.literal("?[" + index + "]");
    }

    /**
     * Build an expression tree for an aggregate call
     */
    private ExpressionNode buildAggregateExpression(AggregateCall aggCall, RelNode input) {
        String aggName = aggCall.getAggregation().getName();
        List<ExpressionNode> children = new ArrayList<>();

        if (aggCall.getArgList().isEmpty()) {
            // COUNT(*) or similar
            children.add(ExpressionNode.literal("*"));
        } else {
            for (int argIdx : aggCall.getArgList()) {
                children.add(buildExpressionTreeFromIndex(argIdx, input));
            }
        }

        return ExpressionNode.operation(aggName, children);
    }

    /**
     * Extract ALL input columns from the RelNode tree.
     * This includes columns in expressions, WHERE clauses, JOIN conditions, etc.
     */
    private Set<String> extractAllInputColumns(RelNode relNode) {
        Set<String> columns = new LinkedHashSet<>();
        // Build a map of table scans for resolving column indices
        Map<Integer, TableInfo> tableMap = buildTableMap(relNode);
        extractColumnsRecursive(relNode, columns, tableMap, 0);
        return columns;
    }

    /**
     * Info about a table scan for column resolution
     */
    private static class TableInfo {
        final String tableName;
        final RelDataType rowType;
        final int startIndex;

        TableInfo(String tableName, RelDataType rowType, int startIndex) {
            this.tableName = tableName;
            this.rowType = rowType;
            this.startIndex = startIndex;
        }
    }

    /**
     * Build a map of column index ranges to table info
     */
    private Map<Integer, TableInfo> buildTableMap(RelNode relNode) {
        Map<Integer, TableInfo> map = new HashMap<>();
        buildTableMapRecursive(relNode, map, new int[]{0});
        return map;
    }

    private void buildTableMapRecursive(RelNode relNode, Map<Integer, TableInfo> map, int[] offset) {
        if (relNode instanceof TableScan) {
            TableScan scan = (TableScan) relNode;
            String tableName = scan.getTable().getQualifiedName()
                .get(scan.getTable().getQualifiedName().size() - 1);
            int startIdx = offset[0];
            for (int i = 0; i < scan.getRowType().getFieldCount(); i++) {
                map.put(startIdx + i, new TableInfo(tableName, scan.getRowType(), startIdx));
            }
            offset[0] += scan.getRowType().getFieldCount();
        } else {
            for (RelNode input : relNode.getInputs()) {
                buildTableMapRecursive(input, map, offset);
            }
        }
    }

    private void extractColumnsRecursive(RelNode relNode, Set<String> columns,
                                         Map<Integer, TableInfo> tableMap, int baseOffset) {
        if (relNode == null) return;

        // For Project, extract columns from expressions
        if (relNode instanceof Project) {
            Project project = (Project) relNode;
            for (RexNode expr : project.getProjects()) {
                extractColumnsFromRex(expr, columns, relNode.getInput(0), 0);
            }
        }

        // For Filter, extract columns from the condition
        if (relNode instanceof Filter) {
            Filter filter = (Filter) relNode;
            extractColumnsFromRex(filter.getCondition(), columns, filter.getInput(), 0);
        }

        // For Join, extract columns from the condition
        if (relNode instanceof Join) {
            Join join = (Join) relNode;
            if (join.getCondition() != null) {
                extractColumnsFromRex(join.getCondition(), columns, join, 0);
            }
        }

        // Recursively process inputs
        for (RelNode input : relNode.getInputs()) {
            extractColumnsRecursive(input, columns, tableMap, baseOffset);
        }
    }

    /**
     * Extract column references from a RexNode expression
     */
    private void extractColumnsFromRex(RexNode rex, Set<String> columns,
                                       RelNode sourceNode, int baseOffset) {
        if (rex instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) rex;
            int index = ref.getIndex();

            // Map the index back to the source table and column
            String colInfo = resolveColumnFromIndex(sourceNode, index);
            if (colInfo != null) {
                columns.add(colInfo);
            }
        } else if (rex instanceof RexCall) {
            RexCall call = (RexCall) rex;
            for (RexNode operand : call.getOperands()) {
                extractColumnsFromRex(operand, columns, sourceNode, baseOffset);
            }
        }
    }

    /**
     * Resolve a column index back to table.column format
     */
    private String resolveColumnFromIndex(RelNode node, int index) {
        if (node instanceof TableScan) {
            TableScan scan = (TableScan) node;
            String tableName = scan.getTable().getQualifiedName()
                .get(scan.getTable().getQualifiedName().size() - 1);
            RelDataType rowType = scan.getRowType();
            if (index < rowType.getFieldCount()) {
                return tableName + "." + rowType.getFieldList().get(index).getName();
            }
        } else if (node instanceof Join) {
            Join join = (Join) node;
            int leftFieldCount = join.getLeft().getRowType().getFieldCount();
            if (index < leftFieldCount) {
                return resolveColumnFromIndex(join.getLeft(), index);
            } else {
                return resolveColumnFromIndex(join.getRight(), index - leftFieldCount);
            }
        } else if (node instanceof Project) {
            // For project, trace through the project expression if it's a direct reference
            Project project = (Project) node;
            if (index < project.getProjects().size()) {
                RexNode expr = project.getProjects().get(index);
                if (expr instanceof RexInputRef) {
                    return resolveColumnFromIndex(project.getInput(), ((RexInputRef) expr).getIndex());
                }
            }
            return resolveColumnFromIndex(node.getInput(0), index);
        } else if (node instanceof Filter) {
            return resolveColumnFromIndex(node.getInput(0), index);
        } else if (node.getInputs().size() == 1) {
            return resolveColumnFromIndex(node.getInput(0), index);
        }
        return null;
    }

    /**
     * Extract all table names referenced in the query
     */
    private Set<String> extractTablesUsed(RelNode relNode) {
        Set<String> tables = new HashSet<>();
        extractTablesRecursive(relNode, tables);
        return tables;
    }
    
    private void extractTablesRecursive(RelNode relNode, Set<String> tables) {
        if (relNode == null) return;
        
        // If this is a table scan, add the table name
        if (relNode instanceof org.apache.calcite.rel.core.TableScan) {
            org.apache.calcite.rel.core.TableScan scan = 
                (org.apache.calcite.rel.core.TableScan) relNode;
            List<String> qualifiedName = scan.getTable().getQualifiedName();
            tables.add(qualifiedName.get(qualifiedName.size() - 1));
        }
        
        // Recursively process inputs
        for (RelNode input : relNode.getInputs()) {
            extractTablesRecursive(input, tables);
        }
    }
    
    /**
     * Convert validated SQL to relational algebra
     */
    private RelNode convertToRelNode(SqlNode validatedNode, SqlValidator validator) {
        RelDataTypeFactory typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        
        // Create planner and cluster
        VolcanoPlanner planner = new VolcanoPlanner();
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        
        // Create SQL to Rel converter
        SqlToRelConverter.Config config = SqlToRelConverter.config()
            .withTrimUnusedFields(false);
            
        SqlToRelConverter converter = new SqlToRelConverter(
            null,
            validator,
            (CalciteCatalogReader) validator.getCatalogReader(),
            cluster,
            StandardConvertletTable.INSTANCE,
            config
        );
        
        // Convert
        RelRoot root = converter.convertQuery(validatedNode, false, true);
        return root.rel;
    }
    
    /**
     * Create validator with sample schema
     */
    private SqlValidator createValidator() {
        RelDataTypeFactory typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("COMPANY", new CompanySchema());
        
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);
        
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList("COMPANY"),
            typeFactory,
            config
        );
        
        return SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT.withIdentifierExpansion(true)
        );
    }
}
