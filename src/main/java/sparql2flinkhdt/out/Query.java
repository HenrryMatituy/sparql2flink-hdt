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
import java.io.Serializable;
import sparql2flinkhdt.runner.functions.*;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.order.*;
import java.math.*;
import java.util.ArrayList;

public class Query implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(Query.class);

	public static void main(String[] args) throws Exception {

		try {

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

	if (hdt != null) {
	LOG.info("El valor de el objeto hdt es correcto");
	} else {
	LOG.info("El valor de el objeto hdt es Nulo");
	}
	if (hdt.getDictionary() != null){
	LOG.info("Valor de hdt.getDictionary() es: {}", hdt.getDictionary());
	} else {
	LOG.info("El diccionario es nulo. Verificar la inicialización de hdt.");
	}
		ArrayList<TripleID> listTripleID = new ArrayList<>();
		try {
		IteratorTripleID iterator = hdt.getTriples().searchAll();
		while(iterator.hasNext()) {
			TripleID tripleID = new TripleID(iterator.next());
			listTripleID.add(tripleID);
		}

	LOG.info("Busqueda de triples HDT completada");
		} catch (Exception e){
	LOG.info("El error de tripes está en:",e);
		e.printStackTrace();
		}		DataSet<TripleID> dataset = env.fromCollection(listTripleID);

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

		//************ Sink  ************
		sm5.writeAsText(params.get("output")+"Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Programan - DataSet API");
		} catch (Exception e){

		LOG.info("El error ocurre cuando:",e);

e.printStackTrace();
		}

	}
}