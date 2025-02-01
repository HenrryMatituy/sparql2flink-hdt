package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flinkhdt.runner.SerializableDictionary;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector implements KeySelector<SolutionMappingHDT, String> {

	private SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector(SerializableDictionary dictionary, String key) {
		this.dictionary = dictionary;
		this.key = key;
	}

	@Override
	public String getKey(SolutionMappingHDT sm) {
		// Obtener el valor de la variable (por ejemplo, ?label)
		SolutionMappingHDT.MappingValue mappingValue = sm.getValue(key);
		if (mappingValue != null) {
			// Convertir el ID de HDT a un valor legible usando el diccionario
			return dictionary.idToString(mappingValue.getId(), mappingValue.getRole());
		}
		return ""; // Devolver un valor por defecto si no se encuentra la variable
	}
}