package sparql2flinkhdt.out;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.graph.Node;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.*;
import sparql2flinkhdt.runner.functions.order.*;
import sparql2flinkhdt.serializable.*;
import java.util.ArrayList;

public class Query {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("dataset") && !params.has("output")) {
			System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		SerializableDictionary dictionary = LoadTriples.fromDataset(env, params.get("dataset"));

		ArrayList<SerializableTripleID> listTripleID = new ArrayList<>();
		IteratorTripleID iterator = dictionary.getHDT().getTriples().searchAll();
		while (iterator.hasNext()) {
			SerializableTripleID tripleID = new SerializableTripleID(iterator.next());
			listTripleID.add(tripleID);
		}

		DataSet<SerializableTripleID> dataset = env.fromCollection(listTripleID);

		DataSet<SolutionMappingHDT> sm1 = dataset
			.filter(new Triple2Triple(dictionary.getDictionary(), null, "http://xmlns.com/foaf/0.1/name", null))
			.map(new Triple2SolutionMapping("?person", null, "?name"));

		DataSet<SolutionMappingHDT> sm2 = dataset
			.filter(new Triple2Triple(dictionary.getDictionary(), null, "http://xmlns.com/foaf/0.1/mbox", null))
			.map(new Triple2SolutionMapping("?person", null, "?mbox"));

		DataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)
			.where(new JoinKeySelector(new String[]{"?person"}))
			.equalTo(new JoinKeySelector(new String[]{"?person"}))
			.with(new LeftJoin());

		DataSet<SolutionMappingHDT> sm4 = sm3
			.map(new Project(new String[]{"?person", "?name", "?mbox"}));

		DataSet<SolutionMappingURI> sm5 = sm4
			.map(new TripleID2TripleString(dictionary.getDictionary()));

		// Sink
		sm5.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Program - DataSet API");
	}
}