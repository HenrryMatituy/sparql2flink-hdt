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
        Set<Integer> usedIndices = new HashSet<>(); // Para evitar joins duplicados

        // Filtrar solo los triples relevantes para la consulta
        List<Triple> relevantTriples = new ArrayList<>();
        for (Triple triple : listTriplePatterns) {
            String predicate = triple.getPredicate().toString();
            if (!(triple.getSubject().toString().contains("Product16") &&
                    (predicate.contains("productPropertyNumeric1") || predicate.contains("productPropertyNumeric2")))) {
                relevantTriples.add(triple);
            }
        }

        // Generar los triple patterns
        for (Triple triple : relevantTriples) {
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

        if (relevantTriples.size() > 1) {
            int joinIndex = currentIndex;
            int leftIndex = -1;

            // Encontrar el triple con Product16 como sujeto (sm2)
            for (int i = startIndex; i < currentIndex; i++) {
                Triple t = relevantTriples.get(i - startIndex);
                if (!t.getSubject().isVariable() && t.getSubject().toString().contains("Product16")) {
                    leftIndex = i;
                    break;
                }
            }
            if (leftIndex == -1) leftIndex = startIndex;

            // Primer join: sm2 con sm3 por ?prodFeature
            int featureIndex = -1;
            for (int i = startIndex; i < currentIndex; i++) {
                Triple t = relevantTriples.get(i - startIndex);
                if (t.getPredicate().toString().equals("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature") &&
                        t.getSubject().isVariable()) {
                    featureIndex = i;
                    break;
                }
            }

            if (featureIndex != -1 && leftIndex != featureIndex) {
                ArrayList<String> keys = SolutionMapping.getKey(leftIndex, featureIndex);
                String keyString = JoinKeys.keys(keys);

                flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(joinIndex)
                        .append(" = sm").append(leftIndex)
                        .append(".join(sm").append(featureIndex).append(")\n")
                        .append("\t\t\t.where(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                        .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                        .append("\t\t\t.with(new Join());\n\n");

                SolutionMapping.join(joinIndex, leftIndex, featureIndex);
                usedIndices.add(leftIndex);
                usedIndices.add(featureIndex);
                leftIndex = joinIndex;
                joinIndex++;
            }

            // Unir los demás triples, evitando duplicados
            for (int i = startIndex; i < currentIndex; i++) {
                if (usedIndices.contains(i)) continue; // Saltar índices ya usados
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

        // Declarar productId solo si es necesario
        boolean needsProductId = false;
        ExprList exprs = opFilter.getExprs();
        for (Expr expr : exprs) {
            if (expr instanceof E_NotEquals) {
                E_NotEquals ne = (E_NotEquals) expr;
                if (ne.getArg1().isConstant() || ne.getArg2().isConstant()) { // Corrección aquí
                    needsProductId = true;
                    break;
                }
            }
        }
        if (needsProductId) {
            flinkProgram.append("\t\tlong productId = hdt.getDictionary().stringToId(\"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product16\", TripleComponentRole.SUBJECT);\n");
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
            } else if (expr instanceof E_LogicalAnd) {
                E_LogicalAnd and = (E_LogicalAnd) expr;
                Expr left = and.getArg1();
                Expr right = and.getArg2();
                flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"").append(left.toString()).append("\"))");
                flinkProgram.append("\n\t\t\t.filter(new Filter(serializableDictionary, \"").append(right.toString()).append("\"))");
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

    @Override
    public void visit(OpDistinct opDistinct) {
        opDistinct.getSubOp().visit(this);
        int subIndex = SolutionMapping.getIndice() - 1;

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(subIndex);
        String varsDistinct = String.join(", ", variables);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm").append(subIndex)
                .append("\n\t\t\t.distinct(new DistinctKeySelector(new String[]{").append(varsDistinct).append("}));\n\n");

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    @Override
    public void visit(OpOrder opOrder) {
        opOrder.getSubOp().visit(this);
        int subIndex = SolutionMapping.getIndice() - 1;

        List<SortCondition> sortConditions = opOrder.getConditions();
        String order = (sortConditions.get(0).getDirection() == -2) ? "Order.ASCENDING" : "Order.DESCENDING";
        String varName = "\"?" + sortConditions.get(0).getExpression().getVarName() + "\"";

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm").append(subIndex)
                .append("\n\t\t\t.sortPartition(new OrderKeySelector(serializableDictionary, ").append(varName)
                .append("), ").append(order).append(").setParallelism(1);\n\n");

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(subIndex);
        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    @Override
    public void visit(OpSlice opSlice) {
        opSlice.getSubOp().visit(this);
        int subIndex = SolutionMapping.getIndice() - 1;

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm").append(subIndex)
                .append("\n\t\t\t.first(").append(opSlice.getLength()).append(");\n\n");

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(subIndex);
        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    public static String getFlinkProgram() {
        return flinkProgram.toString();
    }

    public static void resetFlinkProgram() {
        flinkProgram.setLength(0);
    }
}