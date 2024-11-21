package sparql2flinkhdt.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public class LogicalQueryPlan2FlinkProgram {

    private Op logicalQueryPlan;
    private String className;

    public LogicalQueryPlan2FlinkProgram(Op logicalQueryPlan, Path path) {
        this.logicalQueryPlan = logicalQueryPlan;
        this.className = capitalizeFirstLetter(path.getFileName().toString().split("\\.")[0]);
    }

    private String capitalizeFirstLetter(String text) {
        return text.substring(0, 1).toUpperCase() + text.substring(1).toLowerCase();
    }

    public String logicalQueryPlan2FlinkProgram() {
        StringBuilder flinkProgram = new StringBuilder();

        // Header and imports
        flinkProgram.append("package sparql2flinkhdt.out;\n\n")
                .append("import org.apache.flink.api.java.DataSet;\n")
                .append("import org.apache.flink.api.java.ExecutionEnvironment;\n")
                .append("import org.apache.flink.api.java.utils.ParameterTool;\n")
                .append("import org.apache.flink.core.fs.FileSystem;\n")
                .append("import org.rdfhdt.hdt.triples.TripleID;\n")
                .append("import sparql2flinkhdt.runner.SerializableDictionary;\n")
                .append("import sparql2flinkhdt.runner.LoadTriples;\n")
                .append("import sparql2flinkhdt.runner.functions.*;\n\n")
                .append("import java.util.ArrayList;\n\n");

        // Class definition
        flinkProgram.append("public class ").append(className).append(" {\n")
                .append("\tpublic static void main(String[] args) throws Exception {\n\n")
                .append("\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n")
                .append("\t\tif (!params.has(\"dataset\") || !params.has(\"output\")) {\n")
                .append("\t\t\tSystem.out.println(\"Use --dataset and --output to specify paths.\");\n")
                .append("\t\t\treturn;\n\t\t}\n\n");

        // Environment and dictionary setup
        flinkProgram.append("\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n")
                .append("\t\tSerializableDictionary dictionary = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n\n")
                .append("\t\tArrayList<TripleID> listTripleID = new ArrayList<>();\n")
                .append("\t\tdictionary.getHDT().getTriples().searchAll().forEachRemaining(listTripleID::add);\n\n")
                .append("\t\tDataSet<TripleID> dataset = env.fromCollection(listTripleID);\n\n");

        // Visit logical query plan and generate transformations
        ConvertLQP2FlinkProgram visitor = new ConvertLQP2FlinkProgram();
        logicalQueryPlan.visit(visitor);
        flinkProgram.append(visitor.getFlinkProgram());

        // Execution
        flinkProgram.append("\t\tenv.execute(\"SPARQL Query to Flink Program - DataSet API\");\n\t}\n}");

        return flinkProgram.toString();
    }
}
