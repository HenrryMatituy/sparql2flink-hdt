package sparql2flinkhdt.out;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.jena.ext.xerces.impl.dv.xs.XSSimpleTypeDecl;

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
import java.util.List;

public class Query {
	public static void main(String[] args) throws Exception {
		// Parsear los parámetros de entrada
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Verificación de los parámetros necesarios
		if (!params.has("dataset") || !params.has("output")) {
			System.out.println("Use --dataset para especificar la ruta del dataset y --output para especificar la ruta de salida.");
			return;
		}

		// Crear el entorno de ejecución de Flink
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Configuración adicional para Kryo
//		ExecutionConfig config = env.getConfig();
		env.getConfig().registerTypeWithKryoSerializer(Node_Literal.class, JavaSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(XSSimpleTypeDecl.class, JavaSerializer.class);


		// Cargar los triples del dataset HDT
		HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));

		// Convertir los triples de HDT a una lista de TripleID
		ArrayList<TripleID> listTripleID = new ArrayList<>();
		IteratorTripleID iterator = hdt.getTriples().searchAll();
		while (iterator.hasNext()) {
			TripleID tripleID = new TripleID(iterator.next());
			listTripleID.add(tripleID);
		}

		// Crear un DataSet a partir de la lista de triples
		DataSet<TripleID> dataset = env.fromCollection(listTripleID);

		// Crear el diccionario serializable
		SerializableDictionary serializableDictionary = new SerializableDictionary();
		serializableDictionary.loadFromHDTDictionary(hdt.getDictionary());

// Crear el primer conjunto de mapeo de soluciones (sm1) para personas y nombres
		DataSet<SolutionMappingHDT> sm1 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://xmlns.com/foaf/0.1/name", null))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?person", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));  // Mapea el sujeto como ?person
						sm.putMapping("?name", new SolutionMappingHDT.MappingValue(t.getObject(), 3));  // Mapea el objeto como ?name
						return sm;
					}
				});

// Crear el segundo conjunto de mapeo de soluciones (sm2) para personas y correos electrónicos
		DataSet<SolutionMappingHDT> sm2 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://xmlns.com/foaf/0.1/mbox", null))
				.map(new MapFunction<TripleID, SolutionMappingHDT>() {
					@Override
					public SolutionMappingHDT map(TripleID t) {
						SolutionMappingHDT sm = new SolutionMappingHDT();
						sm.putMapping("?person", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));  // Mapea el sujeto como ?person
						sm.putMapping("?mbox", new SolutionMappingHDT.MappingValue(t.getObject(), 3));  // Mapea el objeto como ?mbox
						return sm;
					}
				});

// Realizar un Left Outer Join entre sm1 y sm2
		DataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)
				.where(new JoinKeySelector(new String[]{"?person"}))
				.equalTo(new JoinKeySelector(new String[]{"?person"}))
				.with(new LeftJoin());

// Proyectar las variables ?person, ?name, y ?mbox
		DataSet<SolutionMappingHDT> sm4 = sm3
				.map(new Project(new String[]{"?person", "?name", "?mbox"}));

// Convertir los IDs a URIs usando el diccionario
		DataSet<SolutionMappingURI> sm5 = sm4
				.map(new TripleID2TripleString(serializableDictionary));

// Escribir el resultado a un archivo de texto

//		sm5.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
//				.setParallelism(1);

		sm5
				.map(value -> value.toString()) // Convierte cada elemento a String
				.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);


//		System.out.println("PRUEBA SM5");
//		sm5.print();
//		System.out.println("FIN PRUEBA SM5");
// Ejecutar el job de Flink
		env.execute("SPARQL Query to Flink Program - DataSet API");

	}
}
