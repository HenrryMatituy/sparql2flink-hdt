package sparql2flinkhdt.out;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.ext.xerces.impl.dv.xs.XSSimpleTypeDecl;
import org.apache.jena.graph.Node_Literal;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.*;
import java.util.ArrayList;

public class Query {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("dataset") || !params.has("output")) {
			System.out.println("Use --dataset and --output to specify paths.");
			return;
		}

		// ************ Initialize Environment and Load Data ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().registerTypeWithKryoSerializer(Node_Literal.class, JavaSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(XSSimpleTypeDecl.class, JavaSerializer.class);

		HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));
		SerializableDictionary serializableDictionary = new SerializableDictionary();
		serializableDictionary.loadFromHDTDictionary(hdt.getDictionary());

		ArrayList<TripleID> listTripleID = new ArrayList<>();
		IteratorTripleID iterator = hdt.getTriples().searchAll();
		while (iterator.hasNext()) {
			TripleID tripleID = new TripleID(iterator.next());
			listTripleID.add(tripleID);
		}

		DataSet<TripleID> dataset = env.fromCollection(listTripleID);

		// ************ Applying Transformations ************
		DataSet<SolutionMappingHDT> sm1 = dataset
			.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new Triple2SolutionMapping("?product", "?label"));

		DataSet<SolutionMappingHDT> sm2 = dataset
			.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType11"))
			.map(new Triple2SolutionMapping("?product", null));

		DataSet<SolutionMappingHDT> sm3 = dataset
			.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature40"))
			.map(new Triple2SolutionMapping("?product", null));

		DataSet<SolutionMappingHDT> sm4 = dataset
			.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature417"))
			.map(new Triple2SolutionMapping("?product", null));

		DataSet<SolutionMappingHDT> sm5 = dataset
			.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new Triple2SolutionMapping("?product", "?value1"));

		DataSet<SolutionMappingHDT> sm6 = sm5
			.filter(new Filter(serializableDictionary, "(> ?value1 10)"));

		DataSet<SolutionMappingHDT> sm7 = sm6
			.map(new Project(new String[]{"?product", "?label"}));

		DataSet<SolutionMappingHDT> sm8 = sm7
			.distinct(new DistinctKeySelector(new String[]{"?product", "?label"}));

		DataSet<SolutionMappingHDT> sm9 = sm8
			.sortPartition(new OrderKeySelector(serializableDictionary, "?label"), Order.ASCENDING).setParallelism(1);

		DataSet<SolutionMappingHDT> sm10 = sm9
			.first(10);

		// ************ Write Results ************
		DataSet<SolutionMappingURI> sm11 = sm10
			.map(new TripleID2TripleString(serializableDictionary));

		sm11.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Program - DataSet API");
	}
}