package sparql2flinkhdt.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public class LogicalQueryPlan2FlinkProgram {

    private Op logicalQueryPlan;
    private String className;

    public LogicalQueryPlan2FlinkProgram(Op logicalQueryPlan, Path path) {
        this.logicalQueryPlan = logicalQueryPlan;
        this.className = path.getFileName().toString();
        this.className = this.className.substring(0, this.className.indexOf('.'));
        this.className = this.className.toLowerCase();
        this.className = this.className.substring(0, 1).toUpperCase() + this.className.substring(1);
    }

    public String logicalQueryPlan2FlinkProgram() {
        StringBuilder flinkProgram = new StringBuilder();

        // Generar el encabezado del programa Java
        flinkProgram.append("package sparql2flinkhdt.out;\n\n")
                .append("import org.apache.flink.api.java.DataSet;\n")
                .append("import org.apache.flink.api.common.operators.Order;\n")
                .append("import org.apache.flink.api.java.ExecutionEnvironment;\n")
                .append("import org.apache.flink.api.java.utils.ParameterTool;\n")
                .append("import org.apache.flink.core.fs.FileSystem;\n")
                .append("import org.apache.jena.graph.Node;\n")
                .append("import org.rdfhdt.hdt.hdt.HDT;\n")
                .append("import org.rdfhdt.hdt.triples.IteratorTripleID;\n")
                .append("import org.rdfhdt.hdt.triples.TripleID;\n")
                .append("import sparql2flinkhdt.runner.functions.*;\n")
                .append("import sparql2flinkhdt.runner.LoadTriples;\n")
                .append("import sparql2flinkhdt.runner.functions.order.*;\n")
                .append("import java.util.ArrayList;\n\n")
                .append("public class ").append(className).append(" {\n")
                .append("\tpublic static void main(String[] args) throws Exception {\n\n")
                .append("\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n")
                .append("\t\tif (!params.has(\"dataset\") && !params.has(\"output\")) {\n")
                .append("\t\t\tSystem.out.println(\"Use --dataset to specify dataset path and use --output to specify output path.\");\n")
                .append("\t\t}\n\n")
                .append("\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n")
                .append("\t\tHDT hdt = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n\n")
                .append("\t\tArrayList<TripleID> listTripleID = new ArrayList<>();\n")
                .append("\t\tIteratorTripleID iterator = hdt.getTriples().searchAll();\n")
                .append("\t\twhile (iterator.hasNext()) {\n")
                .append("\t\t\tTripleID tripleID = new TripleID(iterator.next());\n")
                .append("\t\t\tlistTripleID.add(tripleID);\n")
                .append("\t\t}\n\n")
                .append("\t\tDataSet<TripleID> dataset = env.fromCollection(listTripleID);\n\n");

        // Aquí se añaden las transformaciones y clases envoltorio
        flinkProgram.append("\t\tDataSet<SolutionMappingHDT> sm1 = dataset\n")
                .append("\t\t\t.filter(new Triple2Triple(hdt.getDictionary(), null, \"http://xmlns.com/foaf/0.1/name\", null))\n")
                .append("\t\t\t.map(new Triple2SolutionMapping(\"?person\", null, \"?name\"));\n\n")

                .append("\t\tDataSet<SolutionMappingHDT> sm2 = dataset\n")
                .append("\t\t\t.filter(new Triple2Triple(hdt.getDictionary(), null, \"http://xmlns.com/foaf/0.1/mbox\", null))\n")
                .append("\t\t\t.map(new Triple2SolutionMapping(\"?person\", null, \"?mbox\"));\n\n")

                .append("\t\tDataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)\n")
                .append("\t\t\t.where(new JoinKeySelector(new String[]{\"?person\"}))\n")
                .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{\"?person\"}))\n")
                .append("\t\t\t.with(new LeftJoin());\n\n")

                .append("\t\tDataSet<SolutionMappingHDT> sm4 = sm3.map(new Project(new String[]{\"?person\", \"?name\", \"?mbox\"}));\n\n")
                .append("\t\tDataSet<SolutionMappingURI> sm5 = sm4.map(new TripleID2TripleString(hdt.getDictionary()));\n\n")

                .append("\t\t// Sink\n")
                .append("\t\tsm5.writeAsText(params.get(\"output\") + \"").append(className).append("-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n")
                .append("\t\t\t.setParallelism(1);\n\n")
                .append("\t\tenv.execute(\"SPARQL Query to Flink Program - DataSet API\");\n")
                .append("\t}\n}");

        return flinkProgram.toString();
    }
}
