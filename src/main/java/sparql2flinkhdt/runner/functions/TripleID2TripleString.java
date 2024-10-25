package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.jena.graph.Node;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

import java.util.logging.Logger;

public class TripleID2TripleString implements MapFunction<SolutionMappingHDT, SolutionMappingURI> {

    private SerializableDictionary dictionary;
    private static final Logger logger = Logger.getLogger(TripleID2TripleString.class.getName());

    public TripleID2TripleString(SerializableDictionary dictionary) {
        if (dictionary == null) {
            throw new IllegalArgumentException("El diccionario no puede ser null.");
        }
        this.dictionary = dictionary;
    }

    @Override
    public SolutionMappingURI map(SolutionMappingHDT sm) {
        if (sm == null) {
            logger.severe("map: El SolutionMappingHDT de entrada es null.");
            return null;
        }

        SolutionMappingURI smURI = new SolutionMappingURI();
        System.out.println("Iniciando la conversión de SolutionMappingHDT a SolutionMappingURI.");

        for (String var : sm.getMapping().keySet()) {
            MappingValue mappingValue = sm.getMapping().get(var);

            if (mappingValue == null) {
                logger.severe("map: MappingValue es null para la variable: " + var);
                continue;
            }

            // Realizamos la conversión de ID a URI o literal
            Node node = TripleIDConvert.idToString(dictionary, mappingValue);
            if (node == null) {
                logger.severe("map: Node es null para la variable: " + var + ", id: " + mappingValue.getId());
            } else {
                smURI.putMapping(var, node);
                System.out.println("Variable: " + var + ", Nodo convertido: " + node);
            }
        }

        System.out.println("Conversión completada para SolutionMappingURI: " + smURI);
        return smURI;
    }
}
