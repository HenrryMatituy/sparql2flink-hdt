package sparql2flinkhdt.mapper;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import sparql2flinkhdt.runner.SerializableDictionary;

import java.util.ArrayList;
import java.util.List;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {

    private static StringBuilder flinkProgram = new StringBuilder();
    private SerializableDictionary dictionary;

    public ConvertLQP2FlinkProgram(SerializableDictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public void visit(OpBGP opBGP) {
        List<Triple> listTriplePatterns = opBGP.getPattern().getList();
        flinkProgram.append(ConvertTriplePatternGroup.convert(listTriplePatterns, dictionary));
    }

    @Override
    public void visit(OpProject opProject) {
        ArrayList<String> variables = new ArrayList<>();
        StringBuilder varsProject = new StringBuilder();

        opProject.getVars().forEach(var -> {
            String varName = "\"?" + var.getVarName() + "\"";
            varsProject.append(varName).append(", ");
            variables.add(varName);
        });

        if (varsProject.length() > 0) {
            varsProject.setLength(varsProject.length() - 2); // Eliminar la última coma
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

    public static String getFlinkProgram() {
        return flinkProgram.toString();
    }
}
