package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
//import sparql2flinkhdt.runner.SolutionMappingHDT;

public class OrderKeySelector implements KeySelector<SolutionMappingHDT, String> {

	private Dictionary dictionary;  // Diccionario HDT para convertir IDs a strings
	private String key;  // La clave (variable SPARQL) por la cual se ordenará

	public OrderKeySelector(Dictionary dictionary, String key) {
		this.dictionary = dictionary;
		this.key = key;
	}

	@Override
	public String getKey(SolutionMappingHDT sm) {
		// Obtener el valor asociado a la clave (por ejemplo, "?label")
		SolutionMappingHDT.MappingValue mappingValue = sm.getMapping().get(key);

		if (mappingValue != null) {
			// Convertir el valor a una cadena utilizando el diccionario HDT
			return dictionary.idToString(mappingValue.getId(), mappingValue.getRole()).toString();
		} else {
			// Si no hay valor asociado, devolver una cadena vacía
			return "";
		}
	}
}