package sparql2flinkhdt.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public class LogicalQueryPlan2FlinkProgram {

    private Op logicalQueryPlan;
    private String className;

    public LogicalQueryPlan2FlinkProgram(Op logicalQueryPlan, Path path){
        this.logicalQueryPlan = logicalQueryPlan;
        this.className = path.getFileName().toString();
        this.className = this.className.substring(0, this.className.indexOf('.'));
        this.className = this.className.toLowerCase();
        this.className = this.className.substring(0, 1).toUpperCase() + this.className.substring(1, this.className.length());
    }

    public String logicalQueryPlan2FlinkProgram() {
        String flinkProgram = "";

        flinkProgram += "package sparql2flinkhdt.out;\n\n" +
                "import org.apache.flink.api.java.DataSet;\n" +
                "import org.apache.flink.api.common.operators.Order;\n" +
                "import org.apache.flink.api.java.ExecutionEnvironment;\n" +
                "import org.apache.flink.api.java.utils.ParameterTool;\n" +
                "import org.apache.flink.core.fs.FileSystem;\n" +
                "import org.apache.jena.graph.Node;\n" +
                "import org.slf4j.Logger;\n" +
                "import org.slf4j.LoggerFactory;\n" +

                "import org.rdfhdt.hdt.hdt.HDT;\n" +
                "import org.rdfhdt.hdt.triples.IteratorTripleID;\n" +
                "import org.rdfhdt.hdt.triples.TripleID;\n" +

                "import sparql2flinkhdt.runner.functions.*;\n" +
                "import sparql2flinkhdt.runner.LoadTriples;\n" +
                "import sparql2flinkhdt.runner.functions.order.*;\n" +
                "import java.math.*;\n" +
                "import java.util.ArrayList;\n" +

                "\npublic class "+className+" {\n" +
                "\tprivate static final Logger LOG = LoggerFactory.getLogger(" + className + ".class);\n" +  // Creando
                // el logger
                "\n" +
                "\tpublic static void main(String[] args) throws Exception {\n\n" +
                "\t\ttry {\n\n" +
                "\t\tLOG.info(\"Inicio de la aplicación Henrry\");\n" +
                "\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n" +
                "\t\tif (!params.has(\"dataset\") && !params.has(\"output\")) {\n" +

                "\t\t\tSystem.out.println(\"Use --dataset to specify dataset path and use --output to specify output path.\");\n" +

                "\t\t\tLOG.error(\"Use --dataset to specify dataset path and use --output to specify output path.\");\n" +  // Registrar un error si faltan parámetros
                "\t\t\treturn;\n" +  // Salir del programa en caso de error

                "\t\t}\n\n" +
                "\t\t//************ Environment (DataSet) and Source (static RDF dataset) ************\n" +
                "\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n" +
                "\t\tHDT hdt = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n\n" +

                "\tif (hdt != null) {\n" +
                "\tLOG.info(\"El valor de el objeto hdt es correcto\");\n" +
                "\t} else {\n" +
                "\tLOG.info(\"El valor de el objeto hdt es Nulo\");\n" +
                "\t}\n"+

                "\tif (hdt.getDictionary() != null){\n" +
                "\tLOG.info(\"Valor de hdt.getDictionary() es: {}\", hdt.getDictionary());\n" +  // Sacando el valor de hdt.getDictionary()
                "\t} else {\n" +
                "\tLOG.info(\"El diccionario es nulo. Verificar la inicialización de hdt.\");\n" +
                "\t}\n" +

                "\t\tArrayList<TripleID> listTripleID = new ArrayList<>();\n" +
                "\t\ttry {\n" +
                "\t\tIteratorTripleID iterator = hdt.getTriples().searchAll();\n" +
                "\t\twhile(iterator.hasNext()) {\n" +
                "\t\t\tTripleID tripleID = new TripleID(iterator.next());\n" +
                "\t\t\tlistTripleID.add(tripleID);\n" +
                "\t\t}\n\n" +
                "\tLOG.info(\"Busqueda de triples HDT completada\");\n" +
                "\t\t} catch (Exception e){\n" +
                "\tLOG.info(\"El error de tripes está en:\",e);\n" +
                "\t\te.printStackTrace();\n" +
                "\t\t}" +
                "\t\tDataSet<TripleID> dataset = env.fromCollection(listTripleID);\n\n" +
                "\tLOG.info(\"Número total de triples en el conjunto de datos: {}\", listTripleID.size());\n" +
                "\t\t//************ Applying Transformations ************\n";

        logicalQueryPlan.visit(new ConvertLQP2FlinkProgram());

        flinkProgram += "LOG.info(\" ConvertLQP2FlinkProgram(). Conversión exitosa\\n\");";

        flinkProgram += "LOG.info(\"Obteniendo el programa Flink\\n\");";

        flinkProgram += ConvertLQP2FlinkProgram.getFlinkProgram();

        flinkProgram += "LOG.info(\"Aplicando transformaciones adicionales\\n\");";

        flinkProgram += "\t\tDataSet<SolutionMappingURI> sm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.map(new TripleID2TripleString(hdt.getDictionary()));\n\n";

//        flinkProgram += "LOG.info(\"Transformaciones aplicadas exitosamente\\n\");";
        flinkProgram += "\t\t//************ Sink  ************\n" +
                "\t\tsm"+(SolutionMapping.getIndice()) +
                ".writeAsText(params.get(\"output\")+\""+className+"-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n" +
                "\t\t\t.setParallelism(1);\n\n" +
                "\t\tenv.execute(\"SPARQL Query to Flink Programan - DataSet API\");\n";

        flinkProgram +=  "\t\t} catch (Exception e){\n\n";
        flinkProgram +=  "\t\tLOG.info(\"El error ocurre cuando:\",e);\n\n";
        flinkProgram +=  "e.printStackTrace();\n";
        flinkProgram +=  "\t\t}\n\n";

                flinkProgram+= "\t}\n}";



        return flinkProgram;
    }
}
