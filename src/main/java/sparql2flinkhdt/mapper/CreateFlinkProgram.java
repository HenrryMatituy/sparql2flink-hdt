package sparql2flinkhdt.mapper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class CreateFlinkProgram {

    private String flinkProgram;
    private String fileName;
    private String className;

    public CreateFlinkProgram(String flinkProgram, Path filePath) {
        this.flinkProgram = flinkProgram;
        String fileNameWithExtension = filePath.getFileName().toString();
        this.fileName = fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'));
        this.className = capitalize(this.fileName);
    }

    private String capitalize(String name) {
        if (name == null || name.isEmpty()) return name;
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    public void createFlinkProgram() {
        // Generar el contenido del archivo Java con el nombre de la clase correcto
        String fullProgram = generateFlinkProgram();

        // Ruta para guardar el archivo .java
        Path path = Paths.get("./src/main/java/sparql2flinkhdt/out/" + className + ".java");

        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING))) {
            out.write(fullProgram.getBytes());
            System.out.println("Java Program File << " + className + ".java >> created successfully...");
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    private String generateFlinkProgram() {
        return "package sparql2flinkhdt.out;\n\n" +
                "import org.apache.flink.api.java.DataSet;\n" +
                "import org.apache.flink.api.common.operators.Order;\n" +
                "import org.apache.flink.api.java.ExecutionEnvironment;\n" +
                "import org.apache.flink.api.java.utils.ParameterTool;\n" +
                "import org.apache.flink.core.fs.FileSystem;\n" +
                "import org.apache.jena.graph.Node;\n" +
                "import sparql2flinkhdt.runner.LoadTriples;\n" +
                "import sparql2flinkhdt.runner.functions.*;\n" +
                "import sparql2flinkhdt.runner.functions.order.*;\n" +
                "import sparql2flinkhdt.serializable.*;\n" +
                "import java.util.ArrayList;\n\n" +

                "public class " + className + " {\n" +
                "\tpublic static void main(String[] args) throws Exception {\n\n" +
                "\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n" +
                "\t\tif (!params.has(\"dataset\") && !params.has(\"output\")) {\n" +
                "\t\t\tSystem.out.println(\"Use --dataset to specify dataset path and use --output to specify output path.\");\n" +
                "\t\t}\n\n" +
                "\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n" +
                "\t\tSerializableDictionary dictionary = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n\n" +

                "\t\tArrayList<SerializableTripleID> listTripleID = new ArrayList<>();\n" +
                "\t\tIteratorTripleID iterator = dictionary.getHDT().getTriples().searchAll();\n" +
                "\t\twhile (iterator.hasNext()) {\n" +
                "\t\t\tSerializableTripleID tripleID = new SerializableTripleID(iterator.next());\n" +
                "\t\t\tlistTripleID.add(tripleID);\n" +
                "\t\t}\n\n" +

                "\t\tDataSet<SerializableTripleID> dataset = env.fromCollection(listTripleID);\n\n" +

                "\t\tDataSet<SolutionMappingHDT> sm1 = dataset\n" +
                "\t\t\t.filter(new Triple2Triple(dictionary.getDictionary(), null, \"http://xmlns.com/foaf/0.1/name\", null))\n" +
                "\t\t\t.map(new Triple2SolutionMapping(\"?person\", null, \"?name\"));\n\n" +

                "\t\tDataSet<SolutionMappingHDT> sm2 = dataset\n" +
                "\t\t\t.filter(new Triple2Triple(dictionary.getDictionary(), null, \"http://xmlns.com/foaf/0.1/mbox\", null))\n" +
                "\t\t\t.map(new Triple2SolutionMapping(\"?person\", null, \"?mbox\"));\n\n" +

                "\t\tDataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)\n" +
                "\t\t\t.where(new JoinKeySelector(new String[]{\"?person\"}))\n" +
                "\t\t\t.equalTo(new JoinKeySelector(new String[]{\"?person\"}))\n" +
                "\t\t\t.with(new LeftJoin());\n\n" +

                "\t\tDataSet<SolutionMappingHDT> sm4 = sm3\n" +
                "\t\t\t.map(new Project(new String[]{\"?person\", \"?name\", \"?mbox\"}));\n\n" +

                "\t\tDataSet<SolutionMappingURI> sm5 = sm4\n" +
                "\t\t\t.map(new TripleID2TripleString(dictionary.getDictionary()));\n\n" +

                "\t\t// Sink\n" +
                "\t\tsm5.writeAsText(params.get(\"output\") + \"" + className + "-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n" +
                "\t\t\t.setParallelism(1);\n\n" +

                "\t\tenv.execute(\"SPARQL Query to Flink Program - DataSet API\");\n" +
                "\t}\n" +
                "}";
    }
}
