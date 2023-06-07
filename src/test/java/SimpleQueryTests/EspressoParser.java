package SimpleQueryTests;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import SimpleQueryTests.tableSchema.EMP;

import java.lang.reflect.Type;

public class EspressoParser {
        public static final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        public static final SchemaPlus defaultSchema = Frameworks.createRootSchema(true);

        private FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(defaultSchema).build();
        private Planner planner = Frameworks.getPlanner(config);

        public EspressoParser(){
        addTableSchema();
        }

        public void addTableSchema(){
            SqlToRelConverter.configBuilder().build();
            defaultSchema.add("EMP", new EMP());
            defaultSchema.add("DEPT",new DEPT());
            defaultSchema.add("BONUS",new BONUS());
            defaultSchema.add("ACCOUNT",new ACCOUNT());
        }

        public RelNode getRelNode(String sql) throws SqlParseException, ValidationException, RelConversionException{
            SqlNode parse = planner.parse(sql);
            SqlToRelConverter.configBuilder().build();
            SqlNode validate = planner.validate(parse);
            RelNode tree = planner.rel(validate).rel;
            return tree;
        }
    }
