package sparql2flinkhdt.mapper;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {
    private static String flinkProgram = "";

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

            flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + indice + " = dataset\n" +
                    "\t\t\t.filter(new Triple2Triple(serializableDictionary, " + subjectFilter + ", " + predicateFilter + ", " + objectFilter + "))\n" +
                    "\t\t\t.map(new MapFunction<TripleID, SolutionMappingHDT>() {\n" +
                    "\t\t\t\t@Override\n" +
                    "\t\t\t\tpublic SolutionMappingHDT map(TripleID t) {\n" +
                    "\t\t\t\t\tSolutionMappingHDT sm = new SolutionMappingHDT();\n";

            if (!subjectMapping.equals("null")) {
                flinkProgram += "\t\t\t\t\tsm.putMapping(" + subjectMapping + ", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));\n";
            }
            if (!objectMapping.equals("null")) {
                flinkProgram += "\t\t\t\t\tsm.putMapping(" + objectMapping + ", new SolutionMappingHDT.MappingValue(t.getObject(), 3));\n";
            }

            flinkProgram += "\t\t\t\t\treturn sm;\n" +
                    "\t\t\t\t}\n" +
                    "\t\t\t});\n\n";

            ArrayList<String> variables = new ArrayList<>();
            if (!subjectMapping.equals("null")) variables.add(subjectMapping);
            if (!objectMapping.equals("null")) variables.add(objectMapping);
            SolutionMapping.insertSolutionMapping(indice, variables);

            indice++;
        }
    }
    @Override
    public void visit(OpJoin opJoin) {
        opJoin.getLeft().visit(this);
        int leftIndex = SolutionMapping.getIndice()-1;

        opJoin.getRight().visit(this);
        int rightIndex = SolutionMapping.getIndice()-1;

        int joinIndex = SolutionMapping.getIndice();
        ArrayList<String> listKeys = SolutionMapping.getKey(leftIndex, rightIndex);

        if(listKeys.size() > 0) {
            String keys = JoinKeys.keys(listKeys);
            flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + joinIndex + " = sm" + leftIndex + ".join(sm" + rightIndex + ")\n" +
                    "\t\t\t.where(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                    "\t\t\t.with(new Join());\n\n";
        } else {
            flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + joinIndex + " = sm" + leftIndex + ".cross(sm" + rightIndex + ")\n" +
                    "\t\t\t.with(new Cross());\n\n";
        }

        SolutionMapping.join(joinIndex, leftIndex, rightIndex);
    }
//    @Override
//    public void visit(OpLeftJoin opLeftJoin) {
//        opLeftJoin.getLeft().visit(this);
//        int leftIndex = SolutionMapping.getIndice()-1;
//
//        opLeftJoin.getRight().visit(this);
//        int rightIndex = SolutionMapping.getIndice()-1;
//
//        int joinIndex = SolutionMapping.getIndice();
//        ArrayList<String> listKeys = SolutionMapping.getKey(leftIndex, rightIndex);
//
//        if(listKeys.size() > 0) {
//            String keys = JoinKeys.keys(listKeys);
//            flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + joinIndex + " = sm" + leftIndex + ".leftOuterJoin(sm" + rightIndex + ")\n" +
//                    "\t\t\t.where(new JoinKeySelector(new String[]{" + keys + "}))\n" +
//                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{" + keys + "}))\n" +
//                    "\t\t\t.with(new LeftJoin());\n\n";
//        } else {
//            // Para OPTIONAL sin variables compartidas, usamos cross
//            flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + joinIndex + " = sm" + leftIndex + ".leftOuterJoin(sm" + rightIndex + ")\n" +
//                    "\t\t\t.where(\"*\")\n" +
//                    "\t\t\t.equalTo(\"*\")\n" +
//                    "\t\t\t.with(new LeftJoin());\n\n";
//        }
//
//        // Actualizar el mapa de soluciones
//        SolutionMapping.join(joinIndex, leftIndex, rightIndex);
//
//        // Manejar expresiones de filtro si existen
//        if(opLeftJoin.getExprs() != null) {
//            handleExprList(opLeftJoin.getExprs());
//        }
//    }
//
//    private void handleExprList(ExprList exprList) {
//        for (Expr expression : exprList) {
//            int currentIndex = SolutionMapping.getIndice();
//            flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + currentIndex + " = sm" + (currentIndex-1) + "\n" +
//                    "\t\t\t.filter(new Filter(serializableDictionary, \"" + FilterConvert.convert(expression) + "\"));\n\n";
//
//            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(currentIndex-1);
//            SolutionMapping.insertSolutionMapping(currentIndex, variables);
//        }
//    }


    
    @Override
    public void visit(OpFilter opFilter) {
        ExprList exprList = opFilter.getExprs();
        opFilter.getSubOp().visit(this);
        for (Expr expression : exprList) {
            flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + SolutionMapping.getIndice() + " = sm" + (SolutionMapping.getIndice()-1) + "\n" +
                    "\t\t\t.filter(new Filter(serializableDictionary, \"" + FilterConvert.convert(expression) + "\"));\n\n";

            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);
            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
        }
    }

    @Override
    public void visit(OpProject opProject) {
        ArrayList<String> variables = new ArrayList<>();
        String varsProject = "";
        Iterator<Var> iter = opProject.getVars().iterator();

        while (iter.hasNext()) {
            String var = "\"?" + iter.next().getVarName() + "\"";
            varsProject += var;
            if(iter.hasNext()) varsProject += ", ";
            variables.add(var);
        }

        opProject.getSubOp().visit(this);

        flinkProgram += "\t\tDataSet<SolutionMappingHDT> sm" + SolutionMapping.getIndice() + " = sm" + (SolutionMapping.getIndice()-1) + "\n" +
                "\t\t\t.map(new Project(new String[]{" + varsProject + "}));\n\n";

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    public static String getFlinkProgram() {
        return flinkProgram;
    }
}