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
        flinkProgram.append(ConvertTriplePatternGroup.convert(listTriplePatterns));
    }

    @Override
    public void visit(OpJoin opJoin) {
        processBinaryOp(opJoin.getLeft(), opJoin.getRight(), "join", "Join");
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        processBinaryOp(opLeftJoin.getLeft(), opLeftJoin.getRight(), "leftOuterJoin", "LeftJoin");
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

    @Override
    public void visit(OpOrder opOrder) {
        List<SortCondition> sortConditions = opOrder.getConditions();
        String order = (sortConditions.get(0).getDirection() == -2) ? "Order.ASCENDING" : "Order.DESCENDING";

        opOrder.getSubOp().visit(this);

        Expr expression = sortConditions.get(0).getExpression();
        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm")
                .append(SolutionMapping.getIndice())
                .append(" = sm")
                .append(SolutionMapping.getIndice() - 1)
                .append("\n\t\t\t.sortPartition(new OrderKeySelector(serializableDictionary, \"")
                .append(expression)
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
}
