package Schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class ACCOUNT extends BasicTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
        b.add("ACCTNO", typeFactory.createJavaType(Integer.class));
        b.add("TYPE", typeFactory.createJavaType(String.class));
        b.add("BALANCE", typeFactory.createJavaType(String.class));
        return b.build();
    }
}
