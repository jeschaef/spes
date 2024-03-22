package Schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class EMP extends BasicTable {

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
        b.add("EMPNO", typeFactory.createJavaType(Integer.class));
        b.add("ENAME", typeFactory.createJavaType(String.class));
        b.add("JOB", typeFactory.createJavaType(String.class));
        b.add("MGR", typeFactory.createJavaType(Integer.class));
        b.add("HIREDATE", typeFactory.createJavaType(Integer.class));
        b.add("COMM", typeFactory.createJavaType(Integer.class));
        b.add("SAL", typeFactory.createJavaType(Integer.class));
        b.add("DEPTNO", typeFactory.createJavaType(Integer.class));
        b.add("SLACKER", typeFactory.createJavaType(Integer.class));
        return b.build();
    }

}
