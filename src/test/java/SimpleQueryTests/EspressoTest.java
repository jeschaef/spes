package SimpleQueryTests;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import com.google.gson.*;
import com.microsoft.z3.Context;

import AlgeNode.AlgeNode;
import AlgeNodeParser.AlgeNodeParserPair;
import AlgeRule.AlgeRule;

public class EspressoTest {
    public static void main(String[] args){
        String q1 = args[0];
        String q2 = args[1];
        String schema = args[2];
        if (args.length != 3) {
            System.out.println("Usage: EspressoTest <sql1> <sql2> <schema>");
            System.exit(1);
        }
        JsonObject result = verify(q1, q2, schema);

        // Pretty print.
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(result));
    }


    public static JsonObject verify(String sql1, String sql2, String schema){
        JsonObject result = new JsonObject();
        if((contains(sql1))|| (contains(sql2))) {
            result.addProperty("decision","unknown");
            result.addProperty("reason","sql feature not support");
            return result;
        }
        EspressoParser parser = new EspressoParser(schema);
        EspressoParser parser2 = new EspressoParser(schema);
        RelNode logicPlan = null;
        RelNode logicPlan2 = null;
        try {
            logicPlan = parser.getRelNode(sql1);
            logicPlan2 = parser2.getRelNode(sql2);
        }catch (Exception e){
            result.addProperty("decision","unknown");
            result.addProperty("reason","syntax error in sql");
            return result;
        }
        AlgeNode algeExpr = null;
        AlgeNode algeExpr2 = null;
        try {
            System.out.println(RelOptUtil.toString(logicPlan));
            System.out.println(RelOptUtil.toString(logicPlan2));
            Context z3Context = new Context();
            algeExpr = AlgeRule.normalize(AlgeNodeParserPair.constructAlgeNode(logicPlan, z3Context));
            algeExpr2 = AlgeRule.normalize(AlgeNodeParserPair.constructAlgeNode(logicPlan2, z3Context));
        }catch (Exception e){
            result.addProperty("decision","unknown");
            result.addProperty("reason","sql feature not support");
            return result;
        }
        try {
            result.addProperty("decision", String.valueOf(algeExpr.isEq(algeExpr2)));
            result.addProperty("plan1",RelOptUtil.toString(logicPlan));
            result.addProperty("plan2",RelOptUtil.toString(logicPlan2));
        }catch (Exception e){
            result.addProperty("decision","unknown");
            result.addProperty("reason","unknown");
        }finally {
            return result;
        }
    }

    static public boolean contains(String sql){
        String[] keyWords ={"VALUE","EXISTS","ROW","ORDER","CAST","INTERSECT","EXCEPT"," IN "};
        for (String keyWord : keyWords) {
            if (sql.contains(keyWord)) {
                return true;
            }
        }
        return false;
    }
}
