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
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?label", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm2 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www.w3.org/2000/01/rdf-schema#comment", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?comment", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm3 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?p", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm4 = dataset
			.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?p", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
					sm.putMapping("?producer", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm5 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://purl.org/dc/elements/1.1/publisher", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?p", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm6 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?f", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm7 = dataset
			.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?f", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
					sm.putMapping("?productFeature", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm8 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyTextual1", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm9 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual2", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyTextual2", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm10 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual3", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyTextual3", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm11 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyNumeric1", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm12 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyNumeric2", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm13 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual4", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyTextual4", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm14 = sm12.leftOuterJoin(sm13)
			.where(new JoinKeySelector(new String[]{}))
			.equalTo(new JoinKeySelector(new String[]{}))
			.with(new LeftJoin());

		DataSet<SolutionMappingHDT> sm15 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual5", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyTextual5", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm16 = sm14.leftOuterJoin(sm15)
			.where(new JoinKeySelector(new String[]{}))
			.equalTo(new JoinKeySelector(new String[]{}))
			.with(new LeftJoin());

		DataSet<SolutionMappingHDT> sm17 = dataset
			.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product14", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric4", null))
			.map(new MapFunction<TripleID, SolutionMappingHDT>() {
				@Override
				public SolutionMappingHDT map(TripleID t) {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?propertyNumeric4", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				}
			});

		DataSet<SolutionMappingHDT> sm18 = sm16.leftOuterJoin(sm17)
			.where(new JoinKeySelector(new String[]{}))
			.equalTo(new JoinKeySelector(new String[]{}))
			.with(new LeftJoin());

		DataSet<SolutionMappingHDT> sm19 = sm18
			.map(new Project(new String[]{"?label", "?comment", "?producer", "?productFeature", "?propertyTextual1", "?propertyTextual2", "?propertyTextual3", "?propertyNumeric1", "?propertyNumeric2", "?propertyTextual4", "?propertyTextual5", "?propertyNumeric4"}));

		// ************ Write Results ************
		DataSet<SolutionMappingURI> sm20 = sm19
			.map(new TripleID2TripleString(serializableDictionary));

		sm20.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Program - DataSet API");
	}
}