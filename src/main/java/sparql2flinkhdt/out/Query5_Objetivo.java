package sparql2flinkhdt.out;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.api.common.operators.Order;
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

public class Query5_Objetivo {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        if (!params.has("dataset") || !params.has("output")) {
            System.out.println("Use --dataset and --output to specify paths.");
            return;
        }

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

        DataSet<SolutionMappingHDT> sm1 = dataset
                .filter(new Triple2Triple(serializableDictionary, null, "http://www.w3.org/2000/01/rdf-schema#label", null))
                .map(t -> {
                    SolutionMappingHDT sm = new SolutionMappingHDT();
                    sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
                    sm.putMapping("?productLabel", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
                    return sm;
                });

        DataSet<SolutionMappingHDT> sm2 = dataset
                .filter(new Triple2Triple(serializableDictionary, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product16", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
                .map(t -> {
                    SolutionMappingHDT sm = new SolutionMappingHDT();
                    sm.putMapping("?prodFeature", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
                    return sm;
                });

        DataSet<SolutionMappingHDT> sm3 = dataset
                .filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", null))
                .map(t -> {
                    SolutionMappingHDT sm = new SolutionMappingHDT();
                    sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
                    sm.putMapping("?prodFeature", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
                    return sm;
                });

        DataSet<SolutionMappingHDT> sm5 = dataset
                .filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
                .map(t -> {
                    SolutionMappingHDT sm = new SolutionMappingHDT();
                    sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
                    sm.putMapping("?simProperty1", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
                    return sm;
                });

        DataSet<SolutionMappingHDT> sm7 = dataset
                .filter(new Triple2Triple(serializableDictionary, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2", null))
                .map(t -> {
                    SolutionMappingHDT sm = new SolutionMappingHDT();
                    sm.putMapping("?product", new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
                    sm.putMapping("?simProperty2", new SolutionMappingHDT.MappingValue(t.getObject(), 3));
                    return sm;
                });

        DataSet<SolutionMappingHDT> sm8 = sm3.join(sm2)
                .where(new JoinKeySelector(new String[]{"?prodFeature"}))
                .equalTo(new JoinKeySelector(new String[]{"?prodFeature"}))
                .with(new Join());

        DataSet<SolutionMappingHDT> sm9 = sm8.join(sm1)
                .where(new JoinKeySelector(new String[]{"?product"}))
                .equalTo(new JoinKeySelector(new String[]{"?product"}))
                .with(new Join());

        DataSet<SolutionMappingHDT> sm10 = sm9.join(sm5)
                .where(new JoinKeySelector(new String[]{"?product"}))
                .equalTo(new JoinKeySelector(new String[]{"?product"}))
                .with(new Join());

        DataSet<SolutionMappingHDT> sm11 = sm10.join(sm7)
                .where(new JoinKeySelector(new String[]{"?product"}))
                .equalTo(new JoinKeySelector(new String[]{"?product"}))
                .with(new Join());
        sm11.writeAsText(params.get("output") + "sm11-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        long productId = hdt.getDictionary().stringToId("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product16", TripleComponentRole.SUBJECT);
        DataSet<SolutionMappingHDT> sm12a = sm11
                .filter(new Filter(serializableDictionary, "(!= ?product " + productId + ")"));
        sm12a.writeAsText(params.get("output") + "sm12a-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<SolutionMappingHDT> sm12b = sm12a
                .filter(new Filter(serializableDictionary, "(< ?simProperty1 1520)"))
                .filter(new Filter(serializableDictionary, "(> ?simProperty1 120)"));
        sm12b.writeAsText(params.get("output") + "sm12b-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<SolutionMappingHDT> sm12 = sm12b
                .filter(new Filter(serializableDictionary, "(< ?simProperty2 570)"))
                .filter(new Filter(serializableDictionary, "(> ?simProperty2 170)"));
        sm12.writeAsText(params.get("output") + "sm12-result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<SolutionMappingHDT> sm13 = sm12
                .map(new Project(new String[]{"?product", "?productLabel"}));

        DataSet<SolutionMappingHDT> sm14 = sm13


                .distinct(new DistinctKeySelector(new String[]{"?product", "?productLabel"}));

        DataSet<SolutionMappingHDT> sm15 = sm14
                .sortPartition(new OrderKeySelector(serializableDictionary, "?productLabel"), Order.ASCENDING).setParallelism(1);

        DataSet<SolutionMappingHDT> sm16 = sm15
                .first(5);

        DataSet<SolutionMappingURI> sm17 = sm16
                .map(new TripleID2TripleString(serializableDictionary));

        sm17.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("SPARQL Query to Flink Program - DataSet API");
    }
}