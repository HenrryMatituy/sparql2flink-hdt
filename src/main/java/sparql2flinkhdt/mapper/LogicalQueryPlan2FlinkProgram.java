package sparql2flinkhdt.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public class LogicalQueryPlan2FlinkProgram {

    private final Op logicalQueryPlan;
    private final String className;

    public LogicalQueryPlan2FlinkProgram(Op logicalQueryPlan, Path path) {
        this.logicalQueryPlan = logicalQueryPlan;
        this.className = capitalizeFirstLetter(path.getFileName().toString().split("\\.")[0]);
    }

    private String capitalizeFirstLetter(String text) {
        return text.substring(0, 1).toUpperCase() + text.substring(1).toLowerCase();
    }

    public String logicalQueryPlan2FlinkProgram() {
        StringBuilder flinkProgram = new StringBuilder();

        // Encabezado del programa Flink
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
                // Configuración inicial y carga de datos HDT
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
        System.out.println("Logical Query Plan:");
        System.out.println(logicalQueryPlan.toString());

        // Generar transformaciones basadas en el plan lógico
        logicalQueryPlan.visit(new ConvertLQP2FlinkProgram());
        flinkProgram.append(ConvertLQP2FlinkProgram.getFlinkProgram());

        // Agregar proyecciones y joins explícitos
        flinkProgram.append("\t\t// Example of explicit join and projection\n")
                .append("\t\tDataSet<SolutionMappingHDT> sm1 = dataset\n")
                .append("\t\t\t.filter(new Triple2Triple(serializableDictionary, null, \"http://xmlns.com/foaf/0.1/name\", null))\n")
                .append("\t\t\t.map(new MapFunction<TripleID, SolutionMappingHDT>() {\n")
                .append("\t\t\t\t@Override\n")
                .append("\t\t\t\tpublic SolutionMappingHDT map(TripleID t) {\n")
                .append("\t\t\t\t\tSolutionMappingHDT sm = new SolutionMappingHDT();\n")
                .append("\t\t\t\t\tsm.putMapping(\"?person\", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));\n")
                .append("\t\t\t\t\tsm.putMapping(\"?name\", new SolutionMappingHDT.MappingValue(t.getObject(), 3));\n")
                .append("\t\t\t\t\treturn sm;\n")
                .append("\t\t\t\t}\n")
                .append("\t\t\t});\n\n")
                .append("\t\tDataSet<SolutionMappingHDT> sm2 = dataset\n")
                .append("\t\t\t.filter(new Triple2Triple(serializableDictionary, null, \"http://xmlns.com/foaf/0.1/mbox\", null))\n")
                .append("\t\t\t.map(new MapFunction<TripleID, SolutionMappingHDT>() {\n")
                .append("\t\t\t\t@Override\n")
                .append("\t\t\t\tpublic SolutionMappingHDT map(TripleID t) {\n")
                .append("\t\t\t\t\tSolutionMappingHDT sm = new SolutionMappingHDT();\n")
                .append("\t\t\t\t\tsm.putMapping(\"?person\", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));\n")
                .append("\t\t\t\t\tsm.putMapping(\"?mbox\", new SolutionMappingHDT.MappingValue(t.getObject(), 3));\n")
                .append("\t\t\t\t\treturn sm;\n")
                .append("\t\t\t\t}\n")
                .append("\t\t\t});\n\n")
                .append("\t\tDataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)\n")
                .append("\t\t\t.where(new JoinKeySelector(new String[]{\"?person\"}))\n")
                .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{\"?person\"}))\n")
                .append("\t\t\t.with(new LeftJoin());\n\n")
                .append("\t\tDataSet<SolutionMappingHDT> sm4 = sm3\n")
                .append("\t\t\t.map(new Project(new String[]{\"?person\", \"?name\", \"?mbox\"}));\n\n");

        // Agregar sink y ejecución
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
}