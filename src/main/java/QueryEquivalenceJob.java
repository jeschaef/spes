import AlgeNode.AlgeNode;
import AlgeNodeParser.AlgeNodeParserPair;
import AlgeRule.AlgeRule;
import Z3Helper.z3Utility;
import com.microsoft.z3.Context;
import org.apache.calcite.rel.RelNode;

import java.util.Arrays;
import java.util.UUID;

public class QueryEquivalenceJob extends Job<QueryEquivalenceJobResult> {

    public static String[] keyWords = {"VALUE", "EXISTS", "ROW", "ORDER", "CAST", "INTERSECT", "EXCEPT", " IN "};

    private final String sql1;
    private final String sql2;


    public QueryEquivalenceJob(UUID id) {
        super(id, null);
        this.sql1 = null;
        this.sql2 = null;
    }

    public String getSql1() {
        return sql1;
    }

    public String getSql2() {
        return sql2;
    }

    @Override
    public QueryEquivalenceJob call() {
        this.result = new QueryEquivalenceJobResult();

        // Check if "bad" keyword is contained
        if (contains(sql1) || contains(sql2)) {
            return this;
        }

        // Try to compile the queries
        RelNode logicPlan = null, logicPlan2 = null;
        try {
            z3Utility.reset();
            TaskParser parser = new TaskParser();
            TaskParser parser2 = new TaskParser();

            logicPlan = parser.getRelNode(sql1);
            logicPlan2 = parser2.getRelNode(sql2);
            //System.out.println(RelOptUtil.toString(logicPlan));
            //System.out.println(RelOptUtil.toString(logicPlan2));
            this.result.setCompiled(true);
        } catch (Exception e) {
            System.out.println("Failed to compile " + name);
            System.out.println("Exception: " + e);
            result.setMessage("Failed to compile " + name);
            result.setErrorMessage(e.toString());
            result.setCompiled(false);
        }

        // Verify query equivalence
        if (result.isCompiled()) {
            Context z3Context = new Context();
            try {
                // Construct & normalize algebra nodes
                long startTime = System.currentTimeMillis();
                AlgeNode algeExpr = AlgeNodeParserPair.constructAlgeNode(logicPlan, z3Context);
                AlgeNode algeExpr2 = AlgeNodeParserPair.constructAlgeNode(logicPlan2, z3Context);
                algeExpr = AlgeRule.normalize(algeExpr);
                algeExpr2 = AlgeRule.normalize(algeExpr2);
                z3Utility.reset();

                // Query equivalence?
                if (algeExpr.isEq(algeExpr2)) {
                    long stopTime = System.currentTimeMillis();
                    result.setVerificationTime(stopTime - startTime);
                    result.setProven(true);
                } else {
                    result.setProven(false);
//                    cannotProve.println(RelOptUtil.toString(logicPlan));
//                    cannotProve.println(RelOptUtil.toString(logicPlan2));
//                    cannotProve.flush();
                }
                z3Context.close();

            } catch (Exception e) {
                result.setMessage("Bug in code");
                result.setErrorMessage(e.toString());
                z3Context.close();
            }
        }
        return this;
    }

    static public boolean contains(String sql) {
        return Arrays.stream(keyWords).anyMatch(sql::contains);
    }

    @Override
    public String toString() {
        return "QueryEquivalenceJob{" +
                "name='" + name + '\'' +
                ", id=" + id +
                ", sql1='" + sql1 + '\'' +
                ", sql2='" + sql2 + '\'' +
                '}';
    }
}
