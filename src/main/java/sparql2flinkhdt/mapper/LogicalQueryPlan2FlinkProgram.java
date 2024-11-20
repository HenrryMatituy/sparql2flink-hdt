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
        String flinkProgram = "";

        flinkProgram += "package sparql2flinkhdt.out;\n\n" +
                "import com.esotericsoftware.kryo.serializers.JavaSerializer;\n" +
                "import org.apache.flink.api.common.functions.MapFunction;\n" +
                "import org.apache.flink.api.java.DataSet;\n" +
                "import org.apache.flink.api.java.ExecutionEnvironment;\n" +
                "import org.apache.flink.api.java.utils.ParameterTool;\n" +
                "import org.apache.flink.core.fs.FileSystem;\n" +
                "import org.apache.jena.graph.Node_Literal;\n" +
                "import org.apache.jena.ext.xerces.impl.dv.xs.XSSimpleTypeDecl;\n" +
                "import org.rdfhdt.hdt.hdt.HDT;\n" +
                "import org.rdfhdt.hdt.triples.IteratorTripleID;\n" +
                "import org.rdfhdt.hdt.triples.TripleID;\n" +
                "import sparql2flinkhdt.runner.SerializableDictionary;\n" +
                "import sparql2flinkhdt.runner.LoadTriples;\n" +
                "import sparql2flinkhdt.runner.functions.*;\n\n" +
                "import java.util.ArrayList;\n\n" +

                "public class " + className + " {\n" +
                "\tpublic static void main(String[] args) throws Exception {\n\n" +
                "\t\t// Parsear los parámetros de entrada\n" +
                "\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n" +

                "\t\t// Verificación de los parámetros necesarios\n" +
                "\t\tif (!params.has(\"dataset\") || !params.has(\"output\")) {\n" +
                "\t\t\tSystem.out.println(\"Use --dataset y --output para especificar las rutas.\");\n" +
                "\t\t\treturn;\n" +
                "\t\t}\n\n" +

                "\t\t// Crear el entorno de ejecución de Flink\n" +
                "\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n" +
                "\t\tenv.getConfig().registerTypeWithKryoSerializer(Node_Literal.class, JavaSerializer.class);\n" +
                "\t\tenv.getConfig().registerTypeWithKryoSerializer(XSSimpleTypeDecl.class, JavaSerializer.class);\n\n" +

                "\t\t// Cargar los triples del dataset HDT\n" +
                "\t\tHDT hdt = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n\n" +

                "\t\t// Convertir los triples de HDT a una lista de TripleID\n" +
                "\t\tArrayList<TripleID> listTripleID = new ArrayList<>();\n" +
                "\t\tIteratorTripleID iterator = hdt.getTriples().searchAll();\n" +
                "\t\twhile (iterator.hasNext()) {\n" +
                "\t\t\tTripleID tripleID = new TripleID(iterator.next());\n" +
                "\t\t\tlistTripleID.add(tripleID);\n" +
                "\t\t}\n\n" +

                "\t\t// Crear un DataSet a partir de la lista de triples\n" +
                "\t\tDataSet<TripleID> dataset = env.fromCollection(listTripleID);\n\n" +

                "\t\t// Crear el diccionario serializable\n" +
                "\t\tSerializableDictionary serializableDictionary = new SerializableDictionary();\n" +
                "\t\tserializableDictionary.loadFromHDTDictionary(hdt.getDictionary());\n\n" +

                "\t\t// sm1: Primer filtro y mapeo\n" +
                "\t\tDataSet<SolutionMappingHDT> sm1 = dataset\n" +
                "\t\t\t.filter(new Triple2Triple(serializableDictionary, null, \"http://xmlns.com/foaf/0.1/name\", null))\n" +
                "\t\t\t.map(new MapFunction<TripleID, SolutionMappingHDT>() {\n" +
                "\t\t\t\t@Override\n" +
                "\t\t\t\tpublic SolutionMappingHDT map(TripleID t) {\n" +
                "\t\t\t\t\tSolutionMappingHDT sm = new SolutionMappingHDT();\n" +
                "\t\t\t\t\tsm.putMapping(\"?person\", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));\n" +
                "\t\t\t\t\tsm.putMapping(\"?name\", new SolutionMappingHDT.MappingValue(t.getObject(), 3));\n" +
                "\t\t\t\t\treturn sm;\n" +
                "\t\t\t\t}\n" +
                "\t\t\t});\n\n" +

                "\t\t// sm2: Segundo filtro y mapeo\n" +
                "\t\tDataSet<SolutionMappingHDT> sm2 = dataset\n" +
                "\t\t\t.filter(new Triple2Triple(serializableDictionary, null, \"http://xmlns.com/foaf/0.1/mbox\", null))\n" +
                "\t\t\t.map(new MapFunction<TripleID, SolutionMappingHDT>() {\n" +
                "\t\t\t\t@Override\n" +
                "\t\t\t\tpublic SolutionMappingHDT map(TripleID t) {\n" +
                "\t\t\t\t\tSolutionMappingHDT sm = new SolutionMappingHDT();\n" +
                "\t\t\t\t\tsm.putMapping(\"?person\", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));\n" +
                "\t\t\t\t\tsm.putMapping(\"?mbox\", new SolutionMappingHDT.MappingValue(t.getObject(), 3));\n" +
                "\t\t\t\t\treturn sm;\n" +
                "\t\t\t\t}\n" +
                "\t\t\t});\n\n" +

                "\t\t// Realizar un Left Outer Join entre sm1 y sm2\n" +
                "\t\tDataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)\n" +
                "\t\t\t.where(new JoinKeySelector(new String[]{\"?person\"}))\n" +
                "\t\t\t.equalTo(new JoinKeySelector(new String[]{\"?person\"}))\n" +
                "\t\t\t.with(new LeftJoin());\n\n" +

                "\t\t// Proyectar las variables ?person, ?name, y ?mbox\n" +
                "\t\tDataSet<SolutionMappingHDT> sm4 = sm3\n" +
                "\t\t\t.map(new Project(new String[]{\"?person\", \"?name\", \"?mbox\"}));\n\n" +

                "\t\t// Convertir los IDs a URIs usando el diccionario\n" +
                "\t\tDataSet<SolutionMappingURI> sm5 = sm4\n" +
                "\t\t\t.map(new TripleID2TripleString(serializableDictionary));\n\n" +

                "\t\t//************ Sink ************\n" +
                "\t\tsm5.writeAsText(params.get(\"output\") + \"" + className + "-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n" +
                "\t\t\t.setParallelism(1);\n\n" +

                "\t\t// Ejecutar el job de Flink\n" +
                "\t\tenv.execute(\"SPARQL Query to Flink Program - DataSet API\");\n" +
                "\t}\n}";

        return flinkProgram;
    }
}
