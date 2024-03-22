package Schema;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;

public class DEPT extends BasicTable{
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
        b.add("DEPTNO", typeFactory.createJavaType(Integer.class));
        b.add("NAME", typeFactory.createJavaType(String.class));
        return b.build();
    }
}
