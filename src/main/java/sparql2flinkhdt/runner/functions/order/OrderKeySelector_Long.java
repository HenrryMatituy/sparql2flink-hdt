package sparql2flinkhdt.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Long implements KeySelector<SolutionMappingHDT, Long> {

	private static SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector_Long(SerializableDictionary dictionary, String k) {
		OrderKeySelector_Long.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public Long getKey(SolutionMappingHDT sm) {
		// Obtener el MappingValue del mapping
		MappingValue mappingValue = sm.getMapping().get(key);

		// Verificar que mappingValue exista
		if (mappingValue == null) {
			throw new IllegalArgumentException("No se encontró mapeo para la clave: " + key);
		}

		// Convertir el MappingValue en un Node utilizando TripleIDConvert.idToStringFilter
		Node node = TripleIDConvert.idToStringFilter(dictionary, mappingValue);

		// Obtener el valor literal o URI para usar como clave
		String literalValue;

		if (node.isLiteral()) {
			// Si es un literal, obtenemos su valor
			literalValue = node.getLiteralLexicalForm();
		} else if (node.isURI()) {
			// Si es un URI, puedes extraer la parte que necesites
			// Por ejemplo, el nombre local
			literalValue = node.getLocalName();
		} else {
			throw new IllegalArgumentException("El Node no es un literal ni un URI manejable: " + node);
		}

		try {
			return Long.parseLong(literalValue);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("El valor literal no se puede convertir a Long: " + literalValue, e);
		}
	}
}
