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

    private static StringBuilder flinkProgram = new StringBuilder(1024); // Tamaño inicial
    private int bgpIndex = 1;

    public ConvertLQP2FlinkProgram() {
        super();
        flinkProgram.setLength(0); // Reiniciar siempre al crear una nueva instancia
    }

    @Override
    public void visit(OpBGP opBGP) {
        List<Triple> listTriplePatterns = opBGP.getPattern().getList();
        int startIndex = SolutionMapping.getIndice();
        int currentIndex = startIndex;
        Map<String, Integer> variableToIndex = new HashMap<>();

        // Generar DataSets para cada triple pattern
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

        // Realizar joins para patrones obligatorios
        if (listTriplePatterns.size() > 1) {
            int joinIndex = currentIndex;
            int leftIndex = startIndex;

            for (int i = startIndex + 1; i < currentIndex; i++) {
                int rightIndex = i;
                ArrayList<String> keys = SolutionMapping.getKey(leftIndex, rightIndex);
                String keyString = JoinKeys.keys(keys);

                // Si hay claves comunes, usarlas; si no, asumir unión sin clave específica (product14 como implícito)
                String joinKeys = keys.isEmpty() ? "" : keyString;
                flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(joinIndex)
                        .append(" = sm").append(leftIndex)
                        .append(".join(sm").append(rightIndex).append(")\n")
                        .append("\t\t\t.where(new JoinKeySelector(new String[]{").append(joinKeys).append("}))\n")
                        .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{").append(joinKeys).append("}))\n")
                        .append("\t\t\t.with(new Join());\n\n");

                SolutionMapping.join(joinIndex, leftIndex, rightIndex);
                leftIndex = joinIndex;
                joinIndex++;
            }
            currentIndex = joinIndex;
        }

        bgpIndex = currentIndex;
    }


    @Override
    public void visit(OpJoin opJoin) {
        processBinaryOp(opJoin.getLeft(), opJoin.getRight(), "join", "Join");
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        opLeftJoin.getLeft().visit(this);
        int leftIndex = SolutionMapping.getIndice() - 1;

        opLeftJoin.getRight().visit(this);
        int rightIndex = SolutionMapping.getIndice() - 1;

        int joinIndex = SolutionMapping.getIndice();
        ArrayList<String> keys = SolutionMapping.getKey(leftIndex, rightIndex);
        String keyString = JoinKeys.keys(keys);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(joinIndex)
                .append(" = sm")
                .append(leftIndex)
                .append(".leftOuterJoin(sm")
                .append(rightIndex)
                .append(")\n\t\t\t.where(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{").append(keyString).append("}))\n")
                .append("\t\t\t.with(new LeftJoin());\n\n");

        SolutionMapping.join(joinIndex, leftIndex, rightIndex); // Usar join para mantener consistencia
    }

    @Override
    public void visit(OpUnion opUnion) {
        processBinaryOp(opUnion.getLeft(), opUnion.getRight(), "coGroup", "Union");
    }

    @Override
    public void visit(OpProject opProject) {
        ArrayList<String> variables = new ArrayList<>();
        StringBuilder varsProject = new StringBuilder();
        Iterator<Var> iter = opProject.getVars().iterator();

        while (iter.hasNext()) {
            String var = "\"?" + iter.next().getVarName() + "\"";
            varsProject.append(var);
            if (iter.hasNext()) {
                varsProject.append(", ");
            }
            variables.add(var);
        }

        opProject.getSubOp().visit(this);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm")
                .append(SolutionMapping.getIndice() - 1)
                .append("\n\t\t\t.map(new Project(new String[]{")
                .append(varsProject)
                .append("}));\n\n");

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }


    @Override
    public void visit(OpFilter opFilter) {
        opFilter.getSubOp().visit(this);
        int subIndex = SolutionMapping.getIndice() - 1;

        ExprList exprs = opFilter.getExprs();
        for (Expr expr : exprs) {
            if (expr instanceof E_NotEquals) {
                E_NotEquals ne = (E_NotEquals) expr;
                Expr left = ne.getArg1();
                Expr right = ne.getArg2();
                if (left.isConstant() && left.getConstant().asNode().isURI() && right.isVariable()) {
                    String uri = left.getConstant().asNode().getURI();
                    String varName = right.getVarName();
                    flinkProgram.append("\t\tlong ").append(varName).append("Id = hdt.getDictionary().stringToId(\"")
                            .append(uri).append("\", TripleComponentRole.SUBJECT);\n");
                    flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(SolutionMapping.getIndice())
                            .append(" = sm").append(subIndex)
                            .append("\n\t\t\t.filter(new Filter(serializableDictionary, \"?")
                            .append(varName).append(" != ").append(varName).append("Id\"));\n\n");
                }
            } else {
                String exprStr = expr.toString();
                flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(SolutionMapping.getIndice())
                        .append(" = sm").append(subIndex)
                        .append("\n\t\t\t.filter(new Filter(serializableDictionary, \"").append(exprStr).append("\"));\n\n");
            }
            subIndex = SolutionMapping.getIndice();
        }
    }


    public void visit(ExprList exprList) {
        for (Expr expression : exprList) {
            flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                    .append(SolutionMapping.getIndice())
                    .append(" = sm")
                    .append(SolutionMapping.getIndice() - 1)
                    .append("\n\t\t\t.filter(new Filter(serializableDictionary, \"")
                    .append(FilterConvert.convert(expression))
                    .append("\"));\n\n");

            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice() - 1);
            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
        }
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        opDistinct.getSubOp().visit(this);

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice() - 1);
        String varsDistinct = String.join(", ", variables);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm")
                .append(SolutionMapping.getIndice() - 1)
                .append("\n\t\t\t.distinct(new DistinctKeySelector(new String[]{")
                .append(varsDistinct)
                .append("}));\n\n");

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    @Override
    public void visit(OpOrder opOrder) {
        List<SortCondition> sortConditions = opOrder.getConditions();
        String order = (sortConditions.get(0).getDirection() == -2) ? "Order.ASCENDING" : "Order.DESCENDING";
        String varName = sortConditions.get(0).getExpression().toString();

        opOrder.getSubOp().visit(this);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm")
                .append(SolutionMapping.getIndice() - 1)
                .append("\n\t\t\t.sortPartition(new OrderKeySelector(serializableDictionary, \"")
                .append(varName)
                .append("\"), ")
                .append(order)
                .append(").setParallelism(1);\n\n");

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice() - 1);
        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    @Override
    public void visit(OpSlice opSlice) {
        opSlice.getSubOp().visit(this);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm")
                .append(SolutionMapping.getIndice() - 1)
                .append("\n\t\t\t.first(")
                .append(opSlice.getLength())
                .append(");\n\n");

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice() - 1);
        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    private void processBinaryOp(Op left, Op right, String operation, String operatorClass) {
        left.visit(this);
        int leftIndex = SolutionMapping.getIndice() - 1;

        right.visit(this);
        int rightIndex = SolutionMapping.getIndice() - 1;

        int joinIndex = SolutionMapping.getIndice();
        ArrayList<String> keys = SolutionMapping.getKey(leftIndex, rightIndex);
        String keyString = JoinKeys.keys(keys);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(joinIndex)
                .append(" = sm")
                .append(leftIndex)
                .append(".")
                .append(operation)
                .append("(sm")
                .append(rightIndex)
                .append(")\n\t\t\t.where(new JoinKeySelector(new String[]{")
                .append(keyString)
                .append("}))\n\t\t\t.equalTo(new JoinKeySelector(new String[]{")
                .append(keyString)
                .append("}))\n\t\t\t.with(new ")
                .append(operatorClass)
                .append("());\n\n");

        SolutionMapping.join(joinIndex, leftIndex, rightIndex);
    }

    public static String getFlinkProgram() {
        return flinkProgram.toString();
    }

    public static void resetFlinkProgram() {
        flinkProgram.setLength(0);
    }
}