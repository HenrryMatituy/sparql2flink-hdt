package sparql2flinkhdt.mapper;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;

import java.util.*;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {

    private static StringBuilder flinkProgram = new StringBuilder(1024);
    private int bgpIndex = 1;

    public ConvertLQP2FlinkProgram() {
        super();
        flinkProgram.setLength(0);
    }

    @Override
    public void visit(OpBGP opBGP) {
        List<Triple> listTriplePatterns = opBGP.getPattern().getList();
        int startIndex = SolutionMapping.getIndice();
        int currentIndex = startIndex;
        Map<String, Integer> variableToIndex = new HashMap<>();
        Set<Integer> usedIndices = new HashSet<>();

        for (Triple triple : listTriplePatterns) {
            String subjectFilter = triple.getSubject().isVariable() ? "null" : "\"" + triple.getSubject().toString() + "\"";
            String predicateFilter = triple.getPredicate().isVariable() ? "null" : "\"" + triple.getPredicate().toString() + "\"";
            String objectFilter = triple.getObject().isVariable() ? "null" : "\"" + triple.getObject().toString() + "\"";

            String subjectVar = triple.getSubject().isVariable() ? "\"" + triple.getSubject().toString() + "\"" : "null";
            String objectVar = triple.getObject().isVariable() ? "\"" + triple.getObject().toString() + "\"" : "null";

            flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(currentIndex).append(" = dataset\n")
                    .append("\t\t\t.filter(new Triple2Triple(serializableDictionary, ")
                    .append(subjectFilter).append(", ").append(predicateFilter).append(", ").append(objectFilter).append("))\n")
                    .append("\t\t\t.map(new MapFunction<TripleID, SolutionMappingHDT>() {\n")
                    .append("\t\t\t\t@Override\n")
                    .append("\t\t\t\tpublic SolutionMappingHDT map(TripleID t) {\n")
                    .append("\t\t\t\t\tSolutionMappingHDT sm = new SolutionMappingHDT();\n");

            ArrayList<String> variables = new ArrayList<>();
            if (!subjectVar.equals("null")) {
                flinkProgram.append("\t\t\t\t\tsm.putMapping(").append(subjectVar)
                        .append(", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));\n");
                variables.add(subjectVar);
            }
            if (!objectVar.equals("null")) {
                flinkProgram.append("\t\t\t\t\tsm.putMapping(").append(objectVar)
                        .append(", new SolutionMappingHDT.MappingValue(t.getObject(), 3));\n");
                variables.add(objectVar);
            }

            flinkProgram.append("\t\t\t\t\treturn sm;\n")
                    .append("\t\t\t\t}\n")
                    .append("\t\t\t});\n\n");

            SolutionMapping.insertSolutionMapping(currentIndex, variables);
            variableToIndex.put(triple.getSubject().toString(), currentIndex);
            variableToIndex.put(triple.getObject().toString(), currentIndex);
            currentIndex++;
        }

        if (listTriplePatterns.size() > 1) {
            int joinIndex = currentIndex;
            int leftIndex = startIndex;

            for (int i = startIndex + 1; i < currentIndex; i++) {
                int rightIndex = i;
                ArrayList<String> keys = SolutionMapping.getKey(leftIndex, rightIndex);
                String keyString = JoinKeys.keys(keys);

                flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(joinIndex)
                        .append(" = sm").append(leftIndex)
                        .append(".join(sm").append(rightIndex).append(")\n")
                        .append("\t\t\t.where(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                        .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                        .append("\t\t\t.with(new Join());\n\n");

                SolutionMapping.join(joinIndex, leftIndex, rightIndex);
                usedIndices.add(rightIndex);
                leftIndex = joinIndex;
                joinIndex++;
            }
            bgpIndex = joinIndex;
        } else {
            bgpIndex = currentIndex;
        }
    }

    @Override
    public void visit(OpFilter opFilter) {
        opFilter.getSubOp().visit(this);
        int subIndex = SolutionMapping.getIndice() - 1;

        boolean needsProductId = false;
        ExprList exprs = opFilter.getExprs();
        for (Expr expr : exprs) {
            if (expr instanceof E_NotEquals) {
                E_NotEquals ne = (E_NotEquals) expr;
                if (ne.getArg1().isConstant() || ne.getArg2().isConstant()) {
                    needsProductId = true;
                    break;
                }
            }
        }
        if (needsProductId) {
            flinkProgram.append("\t\tlong productId = hdt.getDictionary().stringToId(\"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product10\", TripleComponentRole.SUBJECT);\n");
        }

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(SolutionMapping.getIndice())
                .append(" = sm").append(subIndex);

        for (Expr expr : exprs) {
            if (expr instanceof E_NotEquals) {
                E_NotEquals ne = (E_NotEquals) expr;
                Expr left = ne.getArg1();
                Expr right = ne.getArg2();
                if (left.isConstant() && left.getConstant().asNode().isURI() && right.isVariable()) {
                    String varName = right.getVarName();
                    flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"(!= ?").append(varName)
                            .append(" productId)\"))");
                } else if (right.isConstant() && right.getConstant().asNode().isURI() && left.isVariable()) {
                    String varName = left.getVarName();
                    flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"(!= ?").append(varName)
                            .append(" productId)\"))");
                } else {
                    String exprStr = expr.toString();
                    flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"").append(exprStr).append("\"))");
                }
            } else if (expr instanceof E_GreaterThanOrEqual) {
                E_GreaterThanOrEqual ge = (E_GreaterThanOrEqual) expr;
                Expr left = ge.getArg1();
                Expr right = ge.getArg2();
                if (left.isVariable() && right.isConstant()) {
                    String varName = left.getVarName();
                    String value = right.getConstant().toString().replace("\"", "").replace("^^<http://www.w3.org/2001/XMLSchema#dateTime>", "");
                    flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"(>= ?").append(varName)
                            .append(" \\\"").append(value).append("\\\")\"))");
                }
            } else {
                String exprStr = expr.toString();
                flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"").append(exprStr).append("\"))");
            }
        }
        flinkProgram.append(";\n\n");

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(subIndex);
        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        opLeftJoin.getLeft().visit(this);
        int leftIndex = SolutionMapping.getIndice() - 1;
        opLeftJoin.getRight().visit(this);
        int rightIndex = SolutionMapping.getIndice() - 1;

        ArrayList<String> keys = SolutionMapping.getKey(leftIndex, rightIndex);
        String keyString = JoinKeys.keys(keys);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(SolutionMapping.getIndice())
                .append(" = sm").append(leftIndex)
                .append("\n\t\t\t.leftOuterJoin(sm").append(rightIndex).append(")\n")
                .append("\t\t\t.where(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                .append("\t\t\t.with(new LeftJoin());\n\n");

        int joinIndex = SolutionMapping.getIndice();
        SolutionMapping.join(joinIndex, leftIndex, rightIndex);

        ExprList exprs = opLeftJoin.getExprs();
        if (exprs != null && !exprs.isEmpty()) {
            // Procesar las expresiones directamente como filtros
            flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(SolutionMapping.getIndice())
                    .append(" = sm").append(joinIndex);

            for (Expr expr : exprs) {
                if (expr instanceof E_GreaterThanOrEqual) {
                    E_GreaterThanOrEqual ge = (E_GreaterThanOrEqual) expr;
                    Expr left = ge.getArg1();
                    Expr right = ge.getArg2();
                    if (left.isVariable() && right.isConstant()) {
                        String varName = left.getVarName();
                        String value = right.getConstant().toString().replace("\"", "").replace("^^<http://www.w3.org/2001/XMLSchema#dateTime>", "");
                        flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"(>= ?").append(varName)
                                .append(" \\\"").append(value).append("\\\")\"))");
                    }
                } else {
                    String exprStr = expr.toString();
                    flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"").append(exprStr).append("\"))");
                }
            }
            flinkProgram.append(";\n\n");
            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(joinIndex);
            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
        }
    }

    @Override
    public void visit(OpProject opProject) {
        opProject.getSubOp().visit(this);
        int subIndex = SolutionMapping.getIndice() - 1;

        ArrayList<String> variables = new ArrayList<>();
        StringBuilder varsProject = new StringBuilder();
        Iterator<Var> iter = opProject.getVars().iterator();
        while (iter.hasNext()) {
            String var = "\"?" + iter.next().getVarName() + "\"";
            varsProject.append(var);
            if (iter.hasNext()) varsProject.append(", ");
            variables.add(var);
        }

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm").append(subIndex)
                .append("\n\t\t\t.map(new Project(new String[]{").append(varsProject).append("}));\n\n");

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    public static String getFlinkProgram() {
        return flinkProgram.toString();
    }

    public static void resetFlinkProgram() {
        flinkProgram.setLength(0);
    }
}