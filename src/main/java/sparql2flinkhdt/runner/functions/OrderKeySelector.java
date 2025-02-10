package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import sparql2flinkhdt.runner.SerializableDictionary;


public class OrderKeySelector implements KeySelector<SolutionMappingHDT, String> {

	private SerializableDictionary serializableDictionary;  // Cambiar Dictionary a SerializableDictionary
	private String key;

	public OrderKeySelector(SerializableDictionary serializableDictionary, String key) {
		this.serializableDictionary = serializableDictionary;
		this.key = key;
	}

	@Override
	public String getKey(SolutionMappingHDT sm) {
		// Obtener el valor asociado a la clave (por ejemplo, "?label")
		SolutionMappingHDT.MappingValue mappingValue = sm.getMapping().get(key);

		if (mappingValue != null) {
			// Convertir el valor a una cadena utilizando SerializableDictionary
			TripleComponentRole role = getRoleFromCode(mappingValue.getRole());
			return serializableDictionary.idToString(mappingValue.getId(), role);
		} else {
			// Si no hay valor asociado, devolver una cadena vacía
			return "";
		}
	}

	// Método para convertir un código numérico a TripleComponentRole
	private TripleComponentRole getRoleFromCode(Integer roleCode) {
		if (roleCode == null) {
			throw new IllegalArgumentException("Role code cannot be null");
		}

		switch (roleCode) {
			case 1:
				return TripleComponentRole.SUBJECT;
			case 2:
				return TripleComponentRole.PREDICATE;
			case 3:
				return TripleComponentRole.OBJECT;
			default:
				throw new IllegalArgumentException("Unknown role code: " + roleCode);
		}
	}
}