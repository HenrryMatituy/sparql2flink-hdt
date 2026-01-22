package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import sparql2flinkhdt.runner.SerializableDictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

public class Filter implements FilterFunction<SolutionMappingHDT> {

    private SerializableDictionary dictionary;
    private String condition;

    public Filter(SerializableDictionary dictionary, String condition) {
        if (dictionary == null || condition == null || condition.trim().isEmpty()) {
            throw new IllegalArgumentException("El diccionario y la condición no pueden ser nulos o vacíos.");
        }
        this.dictionary = dictionary;
        this.condition = condition.trim();
    }

    @Override
    public boolean filter(SolutionMappingHDT solutionMapping) {
        if (solutionMapping == null) {
            return false;  // Si el SolutionMappingHDT es nulo, descartar el registro
        }

        // Extraer la variable, el operador y el valor de la condición
        String[] parts = condition.replaceAll("[()]", "").split(" ");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Formato de condición no válido. Se esperaba: '(operador variable valor)'");
        }

        String operator = parts[0].trim();
        String variable = parts[1].trim();
        String valueStr = parts[2].trim();

        // Obtener el valor de la variable desde el SolutionMappingHDT
        SolutionMappingHDT.MappingValue mappingValue = solutionMapping.getMapping().get(variable);
        if (mappingValue == null) {
            return false;  // Si la variable no existe, no se cumple el filtro
        }

        // Convertir el rol de Integer a TripleComponentRole
        TripleComponentRole role = intToTripleComponentRole(mappingValue.getRole());

        // Obtener el valor real desde el diccionario HDT
        String variableValueStr = dictionary.idToString(mappingValue.getId(), role);

        // Intentar comparar como números
        try {
            // Convertir el valor de la variable a número
            double variableValue = parseValue(variableValueStr);
            // Convertir el valor de la condición a número
            double value = parseValue(valueStr);

            // Aplicar el filtro según el operador
            switch (operator) {
                case ">":
                    return variableValue > value;
                case "<":
                    return variableValue < value;
                case "=":
                    // Usar un margen de error para comparar números de punto flotante
                    double epsilon = 0.000001;
                    return Math.abs(variableValue - value) < epsilon;
                case "!=":
                    epsilon = 0.000001;
                    return Math.abs(variableValue - value) >= epsilon;
                default:
                    throw new IllegalArgumentException("Operador no soportado: " + operator);
            }
        } catch (NumberFormatException e) {
            // Si no es un número, comparar como cadena
            switch (operator) {
                case "=":
                    return variableValueStr.equals(valueStr);
                case "!=":
                    return !variableValueStr.equals(valueStr);
                default:
                    throw new IllegalArgumentException("Operador no soportado para cadenas: " + operator);
            }
        }
    }

    /**
     * Convierte un valor (cadena) a un número (double).
     * Si el valor es un literal numérico (por ejemplo, "10"^^<http://www.w3.org/2001/XMLSchema#integer>),
     * extrae solo la parte numérica.
     */
    private double parseValue(String valueStr) throws NumberFormatException {
        // Eliminar el tipo de dato si está presente (por ejemplo, "10"^^<http://www.w3.org/2001/XMLSchema#integer>)
        if (valueStr.contains("^^")) {
            valueStr = valueStr.split("\\^\\^")[0].replace("\"", "").trim();
        }
        return Double.parseDouble(valueStr);
    }

    /**
     * Convierte un entero a un TripleComponentRole.
     *
     * @param role El entero que representa el rol (1: SUBJECT, 2: PREDICATE, 3: OBJECT).
     * @return El TripleComponentRole correspondiente.
     * @throws IllegalArgumentException Si el valor no es válido.
     */
    private TripleComponentRole intToTripleComponentRole(int role) {
        switch (role) {
            case 1:
                return TripleComponentRole.SUBJECT;
            case 2:
                return TripleComponentRole.PREDICATE;
            case 3:
                return TripleComponentRole.OBJECT;
            default:
                throw new IllegalArgumentException("Valor de rol no válido: " + role);
        }
    }
}