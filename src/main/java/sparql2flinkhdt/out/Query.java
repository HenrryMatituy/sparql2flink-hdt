package sparql2flinkhdt.out;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.ext.xerces.impl.dv.xs.XSSimpleTypeDecl;
import org.apache.jena.graph.Node_Literal;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.*;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.impl.LiteralLabel;
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
		env.getConfig().registerTypeWithKryoSerializer(XSDDatatype.class, JavaSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(LiteralLabel.class, JavaSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(SolutionMappingHDT.class, JavaSerializer.class);
		// Habilitar depuración de Kryo para identificar el problema
//		env.getConfig().enableKryoDebugging();

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
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						sm.putMapping("?label", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm2 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType19"))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm3 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature151"))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm4 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature151"))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm5 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1", null))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						sm.putMapping("?propertyTextual", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm6 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						sm.putMapping("?p1", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm7 = sm1.join(sm2)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm8 = sm7.join(sm3)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm9 = sm8.join(sm4)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm10 = sm9.join(sm5)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm11 = sm10.join(sm6)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm12 = sm11
				.filter(new Filter(serializableDictionary, "(> ?p1 10)"));

		DataSet<SolutionMappingHDT> sm13 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/2000/01/rdf-schema#label", null))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						sm.putMapping("?label", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm14 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType19"))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm15 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature151"))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm16 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature153"))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm17 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1", null))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						sm.putMapping("?propertyTextual", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm18 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2", null))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
						sm.putMapping("?p2", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
						return sm;
					}
				});

		DataSet<SolutionMappingHDT> sm19 = sm13.join(sm14)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm20 = sm19.join(sm15)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm21 = sm20.join(sm16)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm22 = sm21.join(sm17)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm23 = sm22.join(sm18)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());

		DataSet<SolutionMappingHDT> sm24 = sm23
				.filter(new Filter(serializableDictionary, "(> ?p2 10)"));

		DataSet<SolutionMappingHDT> sm25 = sm12.coGroup(sm24)
				.where(new JoinKeySelector(new String[]{"?product", "?label", "?propertyTextual"}))
				.equalTo(new JoinKeySelector(new String[]{"?product", "?label", "?propertyTextual"}))
				.with(new Union());

		DataSet<SolutionMappingHDT> sm26 = sm25
				.map(new Project(new String[]{"?product", "?label", "?propertyTextual"}));

		DataSet<SolutionMappingHDT> sm27 = sm26
				.distinct(new DistinctKeySelector(new String[]{"?product", "?label", "?propertyTextual"}));

		DataSet<SolutionMappingHDT> sm28 = sm27
				.sortPartition(new OrderKeySelector(serializableDictionary, "?label"), Order.ASCENDING).setParallelism(1);

		DataSet<SolutionMappingHDT> sm29 = sm28
				.first(10);

		// ************ Write Results ************
		DataSet<SolutionMappingURI> sm30 = sm29
				.map(new TripleID2TripleString(serializableDictionary));

		sm30.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);

		env.execute("SPARQL Query to Flink Program - DataSet API");
	}
}