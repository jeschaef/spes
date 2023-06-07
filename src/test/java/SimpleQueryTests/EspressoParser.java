package SimpleQueryTests;

import java.util.Map;
import java.util.HashMap;

import com.google.gson.*;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.ImmutableBitSet;

import SimpleQueryTests.tableSchema.EMP;

import java.lang.reflect.Type;

public class EspressoParser {
    public static final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    public static final SchemaPlus defaultSchema = Frameworks.createRootSchema(true);

    private FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(defaultSchema).build();
    private Planner planner = Frameworks.getPlanner(config);

    public EspressoParser(String schema){
        SqlToRelConverter.configBuilder().build();

        // Parse json schema
        Gson gson = new Gson();
        Object tables = gson.fromJson(schema, Object.class);
        Map<String, Map<String,String>> tablesMap = (Map<String, Map<String,String>>) tables;
        for (Map.Entry<String, Map<String,String>> entry : tablesMap.entrySet()) {
            System.out.println("Making table: " + entry.getKey());
            System.out.println("Schema: " + entry.getValue());

            String tableName = entry.getKey();
            defaultSchema.add(tableName, makeTable(entry.getValue()));
        }
    }

    public RelNode getRelNode(String sql) throws SqlParseException, ValidationException, RelConversionException{
        SqlNode parse = planner.parse(sql);
        SqlToRelConverter.configBuilder().build();
        SqlNode validate = planner.validate(parse);
        RelNode tree = planner.rel(validate).rel;
        return tree;
    }

    private Table makeTable(Map<String, String> tableSchema) {
        // Copied from tableSchema/Emp.java
        class localTable implements Table {
            //            private Map<String, String> localSchema = new HashMap<>(tableSchema);

            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();

                for (Map.Entry<String, String> entry : tableSchema.entrySet()) {
                    String columnName = entry.getKey();
                    switch(entry.getValue()) {
                    case "int":
                        b.add(columnName, typeFactory.createJavaType(Integer.class));
                        break;
                    case "str":
                        b.add(columnName, typeFactory.createJavaType(String.class));
                        break;
                    default:
                        System.out.println("Unknown column type " + entry.getValue());
                        throw new IllegalArgumentException("Unknown column type " + entry.getValue());
                    }
                }
                return b.build();
            }
            @Override
            public boolean isRolledUp(String s) {
                return false;
            }
            @Override
            public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode, CalciteConnectionConfig calciteConnectionConfig) {
                return false;
            }
            public Statistic getStatistic() {
                RelFieldCollation.Direction dir = RelFieldCollation.Direction.ASCENDING;
                RelFieldCollation collation = new RelFieldCollation(0, dir, RelFieldCollation.NullDirection.UNSPECIFIED);
                return Statistics.of(5, ImmutableList.of(ImmutableBitSet.of(0)),
                                     ImmutableList.of(RelCollations.of(collation)));
            }
            public Schema.TableType getJdbcTableType() {
                return Schema.TableType.STREAM;
            }

            public Table stream() {
                return null;
            }
        }

        return new localTable();
    }
}
