package Schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class DEPT extends BasicTable{
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
        b.add("DEPTNO", typeFactory.createJavaType(Integer.class));
        b.add("NAME", typeFactory.createJavaType(String.class));
        return b.build();
    }
}
