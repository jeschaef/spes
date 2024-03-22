package Schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class BONUS extends BasicTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
        b.add("ENAME", typeFactory.createJavaType(Integer.class));
        b.add("JOB", typeFactory.createJavaType(String.class));
        b.add("SAL", typeFactory.createJavaType(String.class));
        b.add("COMM", typeFactory.createJavaType(String.class));
        return b.build();
    }
}
