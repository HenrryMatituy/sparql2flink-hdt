package sparql2flinkhdt.out;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.functions.*;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.order.*;
import java.math.*;
import java.util.ArrayList;

public class Query {
	private static final Logger LOG = LoggerFactory.getLogger(Query.class);

	public static void main(String[] args) throws Exception {

		LOG.info("Inicio de la aplicación Henrry");
		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("dataset") && !params.has("output")) {
			System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
			LOG.error("Use --dataset to specify dataset path and use --output to specify output path.");
			return;
		}

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));

		ArrayList<TripleID> listTripleID = new ArrayList<>();
		IteratorTripleID iterator = hdt.getTriples().searchAll();
		while(iterator.hasNext()) {
			TripleID tripleID = new TripleID(iterator.next());
			listTripleID.add(tripleID);
		}

		LOG.info("Valor de hdt.getDictionary() por Henrry: {}", hdt.getDictionary());
		DataSet<TripleID> dataset = env.fromCollection(listTripleID);

		LOG.info("Número total de triples en el conjunto de datos: {}", listTripleID.size());
		//************ Applying Transformations ************
LOG.info(" ConvertLQP2FlinkProgram(). Conversión exitosa");

LOG.info("Obteniendo el programa Flink");
		DataSet<SolutionMappingHDT> sm1 = dataset
			.filter(new Triple2Triple(hdt.getDictionary(), null, "http://xmlns.com/foaf/0.1/name", null))
			.map(new Triple2SolutionMapping("?person", null, "?name"));

		DataSet<SolutionMappingHDT> sm2 = dataset
			.filter(new Triple2Triple(hdt.getDictionary(), null, "http://xmlns.com/foaf/0.1/mbox", null))
			.map(new Triple2SolutionMapping("?person", null, "?mbox"));

		DataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)
			.where(new JoinKeySelector(new String[]{"?person"}))
			.equalTo(new JoinKeySelector(new String[]{"?person"}))
			.with(new LeftJoin());

		DataSet<SolutionMappingHDT> sm4 = sm3
			.map(new Project(new String[]{"?person", "?name", "?mbox"}));

LOG.info("Aplicando transformaciones adicionales");
		DataSet<SolutionMappingURI> sm5 = sm4
			.map(new TripleID2TripleString(hdt.getDictionary()));

LOG.info("Transformaciones aplicadas exitosamente");
		//************ Sink  ************
		sm5.writeAsText(params.get("output")+"Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Programan - DataSet API");
	}
}