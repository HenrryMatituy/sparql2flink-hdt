package sparql2flinkhdt.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public String logicalQueryPlan2FlinkProgram() {
    StringBuilder flinkProgram = new StringBuilder();

    flinkProgram.append("package sparql2flinkhdt.out;\n\n")
            .append("import com.esotericsoftware.kryo.serializers.JavaSerializer;\n")
            .append("import org.apache.flink.api.common.functions.MapFunction;\n")
            .append("import org.apache.flink.api.java.DataSet;\n")
            .append("import org.apache.flink.api.java.ExecutionEnvironment;\n")
            .append("import org.apache.flink.api.java.utils.ParameterTool;\n")
            .append("import org.apache.flink.core.fs.FileSystem;\n")
            .append("import org.apache.jena.ext.xerces.impl.dv.xs.XSSimpleTypeDecl;\n")
            .append("import org.apache.jena.graph.Node_Literal;\n")
            .append("import org.rdfhdt.hdt.enums.TripleComponentRole;\n")
            .append("import org.rdfhdt.hdt.hdt.HDT;\n")
            .append("import org.rdfhdt.hdt.triples.IteratorTripleID;\n")
            .append("import org.rdfhdt.hdt.triples.TripleID;\n")
            .append("import sparql2flinkhdt.runner.SerializableDictionary;\n")
            .append("import sparql2flinkhdt.runner.LoadTriples;\n")
            .append("import sparql2flinkhdt.runner.functions.*;\n")
            .append("import java.util.ArrayList;\n\n")
            .append("public class ").append(className).append(" {\n")
            .append("\tpublic static void main(String[] args) throws Exception {\n\n")
            .append("\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n")
            .append("\t\tif (!params.has(\"dataset\") || !params.has(\"output\")) {\n")
            .append("\t\t\tSystem.out.println(\"Use --dataset and --output to specify paths.\");\n")
            .append("\t\t\treturn;\n\t\t}\n\n")
            .append("\t\t// ************ Initialize Environment and Load Data ************\n")
            .append("\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n")
            .append("\t\tenv.getConfig().registerTypeWithKryoSerializer(Node_Literal.class, JavaSerializer.class);\n")
            .append("\t\tenv.getConfig().registerTypeWithKryoSerializer(XSSimpleTypeDecl.class, JavaSerializer.class);\n\n")
            .append("\t\tHDT hdt = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n")
            .append("\t\tSerializableDictionary serializableDictionary = new SerializableDictionary();\n")
            .append("\t\tserializableDictionary.loadFromHDTDictionary(hdt.getDictionary());\n\n")
            .append("\t\tArrayList<TripleID> listTripleID = new ArrayList<>();\n")
            .append("\t\tIteratorTripleID iterator = hdt.getTriples().searchAll();\n")
            .append("\t\twhile (iterator.hasNext()) {\n")
            .append("\t\t\tTripleID tripleID = new TripleID(iterator.next());\n")
            .append("\t\t\tlistTripleID.add(tripleID);\n")
            .append("\t\t}\n\n")
            .append("\t\tDataSet<TripleID> dataset = env.fromCollection(listTripleID);\n\n")
            .append("\t\t// ************ Applying Transformations ************\n");

    // Visit Logical Query Plan and append transformations to the program
    logicalQueryPlan.visit(new ConvertLQP2FlinkProgram());
    flinkProgram.append(ConvertLQP2FlinkProgram.getFlinkProgram());

    // Add sink and execution
    flinkProgram.append("\t\t// ************ Write Results ************\n")
            .append("\t\tDataSet<SolutionMappingURI> sm").append(SolutionMapping.getIndice())
            .append(" = sm").append(SolutionMapping.getIndice() - 1)
            .append("\n\t\t\t.map(new TripleID2TripleString(serializableDictionary));\n\n")
            .append("\t\tsm").append(SolutionMapping.getIndice())
            .append(".writeAsText(params.get(\"output\") + \"").append(className).append("-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n")
            .append("\t\t\t.setParallelism(1);\n\n")
            .append("\t\tenv.execute(\"SPARQL Query to Flink Program - DataSet API\");\n\t}\n}");

    return flinkProgram.toString();
}