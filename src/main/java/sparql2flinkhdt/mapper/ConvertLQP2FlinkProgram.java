package sparql2flinkhdt.mapper;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {

    private static StringBuilder flinkProgram = new StringBuilder();

    public ConvertLQP2FlinkProgram() {
        super();
    }

    @Override
    public void visit(OpBGP opBGP) {
        List<Triple> listTriplePatterns = opBGP.getPattern().getList();
        int indice = SolutionMapping.getIndice();

        for (Triple triple : listTriplePatterns) {
            String subjectFilter = triple.getSubject().isVariable() ? "null" : "\"" + triple.getSubject().toString() + "\"";
            String predicateFilter = triple.getPredicate().isVariable() ? "null" : "\"" + triple.getPredicate().toString() + "\"";
            String objectFilter = triple.getObject().isVariable() ? "null" : "\"" + triple.getObject().toString() + "\"";

            String subjectMapping = triple.getSubject().isVariable() ? "\"" + triple.getSubject().toString() + "\"" : "null";
            String objectMapping = triple.getObject().isVariable() ? "\"" + triple.getObject().toString() + "\"" : "null";

            flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm").append(indice).append(" = dataset\n")
                    .append("\t\t\t.filter(new Triple2Triple(serializableDictionary, ")
                    .append(subjectFilter).append(", ")
                    .append(predicateFilter).append(", ")
                    .append(objectFilter).append("))\n")
                    .append("\t\t\t.map(new MapFunction<TripleID, SolutionMappingHDT>() {\n")
                    .append("\t\t\t\t@Override\n")
                    .append("\t\t\t\tpublic SolutionMappingHDT map(TripleID t) {\n")
                    .append("\t\t\t\t\tSolutionMappingHDT sm = new SolutionMappingHDT();\n");

            if (!subjectMapping.equals("null")) {
                flinkProgram.append("\t\t\t\t\tsm.putMapping(")
                        .append(subjectMapping)
                        .append(", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));\n");
            }
            if (!objectMapping.equals("null")) {
                flinkProgram.append("\t\t\t\t\tsm.putMapping(")
                        .append(objectMapping)
                        .append(", new SolutionMappingHDT.MappingValue(t.getObject(), 3));\n");
            }

            flinkProgram.append("\t\t\t\t\treturn sm;\n")
                    .append("\t\t\t\t}\n")
                    .append("\t\t\t});\n\n");

            ArrayList<String> variables = new ArrayList<>();
            if (!subjectMapping.equals("null")) variables.add(subjectMapping);
            if (!objectMapping.equals("null")) variables.add(objectMapping);
            SolutionMapping.insertSolutionMapping(indice, variables);

            indice++;
        }
    }

    @Override
    public void visit(OpJoin opJoin) {
        logProgress("Processing join operation");

        opJoin.getLeft().visit(this);
        int leftIndex = SolutionMapping.getIndice() - 1;
        logProgress("Left variables: " + SolutionMapping.getSolutionMapping().get(leftIndex));

        opJoin.getRight().visit(this);
        int rightIndex = SolutionMapping.getIndice() - 1;
        logProgress("Right variables: " + SolutionMapping.getSolutionMapping().get(rightIndex));

        int joinIndex = SolutionMapping.getIndice();
        ArrayList<String> keys = SolutionMapping.getKey(leftIndex, rightIndex);

        if (keys.isEmpty()) {
            logProgress("No common variables found, using cross join");
            flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                    .append(joinIndex)
                    .append(" = sm")
                    .append(leftIndex)
                    .append(".cross(sm")
                    .append(rightIndex)
                    .append(")\n")
                    .append("\t\t\t.with(new Cross());\n\n");
        } else {
            logProgress("Common variables found: " + keys);
            String keyString = JoinKeys.keys(keys);
            flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                    .append(joinIndex)
                    .append(" = sm")
                    .append(leftIndex)
                    .append(".join(sm")
                    .append(rightIndex)
                    .append(")\n")
                    .append("\t\t\t.where(new JoinKeySelector(new String[]{")
                    .append(keyString)
                    .append("}))\n")
                    .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{")
                    .append(keyString)
                    .append("}))\n")
                    .append("\t\t\t.with(new Join());\n\n");
        }

        SolutionMapping.join(joinIndex, leftIndex, rightIndex);
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
                .append(")\n\t\t\t.where(new JoinKeySelector(new String[]{")
                .append(keyString)
                .append("}))\n\t\t\t.equalTo(new JoinKeySelector(new String[]{")
                .append(keyString)
                .append("}))\n\t\t\t.with(new LeftJoin());\n\n");

        SolutionMapping.join(joinIndex, leftIndex, rightIndex);

        // Si hay filtros adicionales
        if (opLeftJoin.getExprs() != null) {
            visit(opLeftJoin.getExprs());
        }
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

        // Llamar al subárbol
        opProject.getSubOp().visit(this);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm")
                .append(SolutionMapping.getIndice() - 1)
                .append("\n\t\t\t.map(new Project(new String[]{")
                .append(varsProject)
                .append("}));\n\n");

        // Incrementar índice para reflejar el nuevo sm
        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }


    @Override
    public void visit(OpFilter opFilter) {
        ExprList exprList = opFilter.getExprs();
        opFilter.getSubOp().visit(this);
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
    private void processBinaryOp(Op left, Op right, String operation, String operatorClass) {
        // Añadir logging para debug
        logProgress("Processing binary operation: " + operatorClass);

        left.visit(this);
        int leftIndex = SolutionMapping.getIndice() - 1;

        right.visit(this);
        int rightIndex = SolutionMapping.getIndice() - 1;

        // Logging de variables
        logProgress("Left variables: " + SolutionMapping.getSolutionMapping().get(leftIndex));
        logProgress("Right variables: " + SolutionMapping.getSolutionMapping().get(rightIndex));

        int joinIndex = SolutionMapping.getIndice();
        ArrayList<String> keys = SolutionMapping.getKey(leftIndex, rightIndex);

        // Si no hay keys comunes en un join normal, podría ser un error
        if (keys.isEmpty() && operatorClass.equals("Join")) {
            logProgress("Warning: No common variables found for join operation");
        }

        String keyString = JoinKeys.keys(keys);

        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(joinIndex)
                .append(" = sm")
                .append(leftIndex)
                .append(".")
                .append(operation)
                .append("(sm")
                .append(rightIndex)
                .append(")\n");

        // Solo añadir where/equalTo si hay keys
        if (!keys.isEmpty()) {
            flinkProgram.append("\t\t\t.where(new JoinKeySelector(new String[]{")
                    .append(keyString)
                    .append("}))\n\t\t\t.equalTo(new JoinKeySelector(new String[]{")
                    .append(keyString)
                    .append("}))\n");
        }

        flinkProgram.append("\t\t\t.with(new ")
                .append(operatorClass)
                .append("());\n\n");

        SolutionMapping.join(joinIndex, leftIndex, rightIndex);
    }

    private void logProgress(String message) {
        System.out.println("[DEBUG] " + message);
    }
    
    public static String getFlinkProgram() {
        return flinkProgram.toString();
    }
}