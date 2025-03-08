package sparql2flinkhdt.out;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
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
import org.rdfhdt.hdt.enums.TripleComponentRole;
import java.util.ArrayList;
import java.io.PrintWriter;

public class Query {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		PrintWriter logWriter = new PrintWriter(params.get("output") + "debug-log.txt", "UTF-8");

		if (!params.has("dataset") || !params.has("output")) {
			logWriter.println("Use --dataset and --output to specify paths.");
			logWriter.close();
			return;
		}

		logWriter.println("Inicio del programa");

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().registerTypeWithKryoSerializer(Node_Literal.class, JavaSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(XSSimpleTypeDecl.class, JavaSerializer.class);
		logWriter.println("Entorno inicializado");

		HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));
		logWriter.println("HDT cargado");

		SerializableDictionary serializableDictionary = new SerializableDictionary();
		serializableDictionary.loadFromHDTDictionary(hdt.getDictionary());
		logWriter.println("Diccionario serializable cargado");

		ArrayList<TripleID> listTripleID = new ArrayList<>();
		IteratorTripleID iterator = hdt.getTriples().searchAll();
		while (iterator.hasNext()) {
			TripleID tripleID = new TripleID(iterator.next());
			listTripleID.add(tripleID);
		}
		logWriter.println("Triples cargados: " + listTripleID.size());

		DataSet<TripleID> dataset = env.fromCollection(listTripleID);
		logWriter.println("Dataset creado");

		// Patrones básicos
		DataSet<SolutionMappingHDT> sm1 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/2000/01/rdf-schema#label", null))
				.map(t -> {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
					sm.putMapping("?productLabel", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				});
		logWriter.println("sm1 creado");

		DataSet<SolutionMappingHDT> sm2 = dataset
				.filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product16", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
				.map(t -> {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?prodFeature", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				});
		logWriter.println("sm2 creado");

		DataSet<SolutionMappingHDT> sm3 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
				.map(t -> {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
					sm.putMapping("?prodFeature", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				});
		logWriter.println("sm3 creado");

		DataSet<SolutionMappingHDT> sm4 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
				.map(t -> {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
					sm.putMapping("?simProperty1", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				});
		logWriter.println("sm4 creado");

		DataSet<SolutionMappingHDT> sm5 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2", null))
				.map(t -> {
					SolutionMappingHDT sm = new SolutionMappingHDT();
					sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
					sm.putMapping("?simProperty2", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
					return sm;
				});
		logWriter.println("sm5 creado");

		// Joins
		DataSet<SolutionMappingHDT> sm6 = sm3.join(sm2)
				.where(new JoinKeySelector(new String[]{"?prodFeature"}))
				.equalTo(new JoinKeySelector(new String[]{"?prodFeature"}))
				.with(new Join());
		sm6.writeAsText(params.get("output") + "sm6-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		logWriter.println("sm6 creado");

		DataSet<SolutionMappingHDT> sm7 = sm6.join(sm1)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());
		sm7.writeAsText(params.get("output") + "sm7-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		logWriter.println("sm7 creado");

		DataSet<SolutionMappingHDT> sm8 = sm7.join(sm4)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());
		sm8.writeAsText(params.get("output") + "sm8-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		logWriter.println("sm8 creado");

		DataSet<SolutionMappingHDT> sm9 = sm8.join(sm5)
				.where(new JoinKeySelector(new String[]{"?product"}))
				.equalTo(new JoinKeySelector(new String[]{"?product"}))
				.with(new Join());
		sm9.writeAsText(params.get("output") + "sm9-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		logWriter.println("sm9 creado");

		// Obtener el ID de Product16
		logWriter.println("Antes de obtener Product16 ID");
		long productId = hdt.getDictionary().stringToId("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product16", TripleComponentRole.SUBJECT);
		logWriter.println("Product16 ID: " + productId);

		// Filtro original
		logWriter.println("Antes de aplicar filtro sm10");
		DataSet<SolutionMappingHDT> sm10 = sm9
				.filter(new Filter(serializableDictionary, "(!= " + productId + " ?product)"));
		logWriter.println("Filtro sm10 aplicado (!=)");
		sm10.writeAsText(params.get("output") + "sm10-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// Filtro inverso para depuración
		logWriter.println("Antes de aplicar filtro sm10-inverse");
		DataSet<SolutionMappingHDT> sm10Inverse = sm9
				.filter(new Filter(serializableDictionary, "(== " + productId + " ?product)"));
		logWriter.println("Filtro sm10-inverse aplicado (==)");
		sm10Inverse.writeAsText(params.get("output") + "sm10-inverse-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// Filtro con sintaxis alternativa
		logWriter.println("Antes de aplicar filtro sm10-alt");
		DataSet<SolutionMappingHDT> sm10Alt = sm9
				.filter(new Filter(serializableDictionary, "?product != " + productId));
		logWriter.println("Filtro sm10-alt aplicado (!= alternativo)");
		sm10Alt.writeAsText(params.get("output") + "sm10-alt-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		logWriter.println("Ejecución completada");
		logWriter.close();

		env.execute("SPARQL Query to Flink Program - DataSet API");
	}
}