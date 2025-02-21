//prueba 21 02 2025
package sparql2flinkhdt.runner.functions;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.function.FunctionEnvBase;
import org.apache.jena.sparql.sse.SSE;
import sparql2flinkhdt.runner.SerializableDictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;  // Asegúrate de importar la clase correcta
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SolutionMappingHDT implements Serializable {

    private static final long serialVersionUID = 1L;
    private SerializableDictionary serializableDictionary;
    private static final Logger logger = Logger.getLogger(SolutionMappingHDT.class.getName());

    // Clase interna para representar los valores de mapeo
    public static class MappingValue implements Serializable {
        private static final long serialVersionUID = 1L;
        private Long id;
        private Integer role;  // Usar Integer para el rol

        public MappingValue(Long id, Integer role) {
            if (id == null || role == null) {
                throw new IllegalArgumentException("ID y role no pueden ser nulos.");
            }
            this.id = id;
            this.role = role;
        }

        public Long getId() {
            return id;
        }

        public Integer getRole() {
            return role;
        }

        /**
         * Obtiene el valor real desde el diccionario HDT.
         *
         * @param dictionary El diccionario HDT serializable.
         * @return El valor como una cadena.
         */
        public String getValue(SerializableDictionary dictionary) {
            if (dictionary == null) {
                throw new IllegalArgumentException("El diccionario no puede ser nulo.");
            }
            // Convertir el rol de Integer a TripleComponentRole
            TripleComponentRole role = intToTripleComponentRole(this.role);
            return dictionary.idToString(this.id, role);
        }

        @Override
        public String toString() {
            return "MappingValue{id=" + id + ", role=" + role + "}";
        }
    }

    private HashMap<String, MappingValue> mapping = new HashMap<>();

    public SolutionMappingHDT() {}

    public SolutionMappingHDT(SerializableDictionary serializableDictionary) {
        this.serializableDictionary = serializableDictionary;
    }

    // Getter y Setter para serializableDictionary
    public SerializableDictionary getSerializableDictionary() {
        return serializableDictionary;
    }

    public void setSerializableDictionary(SerializableDictionary serializableDictionary) {
        this.serializableDictionary = serializableDictionary;
    }

    public void setMapping(HashMap<String, MappingValue> mapping) {
        this.mapping = mapping;
    }

    public HashMap<String, MappingValue> getMapping() {
        return mapping;
    }

    public void putMapping(String var, MappingValue val) {
        if (var == null || val == null) {
            logger.warning("Intento de agregar un mapeo con variable o valor nulo.");
            return;
        }

        // Agregar el mapeo al mapa
        mapping.put(var, val);
    }

    public SolutionMappingHDT join(SolutionMappingHDT sm) {
        SolutionMappingHDT result = new SolutionMappingHDT(this.serializableDictionary);
        result.mapping.putAll(this.mapping);

        for (Map.Entry<String, MappingValue> entry : sm.getMapping().entrySet()) {
            String key = entry.getKey();
            MappingValue value = entry.getValue();

            if (!result.mapping.containsKey(key)) {
                result.mapping.put(key, value);
            } else {
                // Manejar conflictos si es necesario
                MappingValue existingValue = result.mapping.get(key);

                // Verificar que los valores no sean nulos
                if (existingValue == null || value == null) {
                    logger.warning("Uno de los valores es nulo. No se puede comparar.");
                    continue; // O manejar el conflicto de otra manera
                }

                // Verificar que los IDs no sean nulos
                if (existingValue.getId() == null || value.getId() == null) {
                    logger.warning("Uno de los IDs es nulo. No se puede comparar.");
                    continue; // O manejar el conflicto de otra manera
                }

                // Comparar los IDs
                if (!existingValue.getId().equals(value.getId())) {
                    logger.warning("Conflicto al unir mappings: variable " + key + " tiene valores diferentes.");
                    // Decidir cómo manejar el conflicto: sobrescribir, ignorar, etc.
                }
            }
        }
        return result;
    }

    public SolutionMappingHDT leftJoin(SolutionMappingHDT right) {
        SolutionMappingHDT result = new SolutionMappingHDT(this.serializableDictionary);
        result.mapping.putAll(this.mapping);

        if (right != null) {
            for (Map.Entry<String, MappingValue> rightEntry : right.getMapping().entrySet()) {
                String var = rightEntry.getKey();
                MappingValue rightValue = rightEntry.getValue();

                if (!result.mapping.containsKey(var)) {
                    result.mapping.put(var, rightValue);
                }
            }
        }

        return result;
    }

    @Override
    public String toString() {
        StringBuilder sm = new StringBuilder();
        for (Map.Entry<String, MappingValue> entry : mapping.entrySet()) {
            if (entry.getValue() != null) {
                sm.append(entry.getKey()).append("-->").append(entry.getValue().getId()).append("\t");
            }
        }
        return sm.toString();
    }

    // Método para aplicar un filtro basado en una expresión SPARQL
    public boolean filter(String expression) {
        // Validar la expresión
        if (expression == null || expression.trim().isEmpty()) {
            throw new IllegalArgumentException("La expresión de filtro no puede estar vacía.");
        }

        // Extraer la variable y el valor de la expresión
        String[] parts = expression.replaceAll("[()]", "").split(" ");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Formato de expresión no válido. Se esperaba: '(operador variable valor)'");
        }

        String operator = parts[0].trim();
        String variable = parts[1].trim();
        String valueStr = parts[2].trim();

        // Obtener el valor de la variable
        MappingValue mappingValue = this.mapping.get(variable);
        if (mappingValue == null) {
            return false;  // Si la variable no existe, no se cumple el filtro
        }

        // Obtener el valor real desde el diccionario
        String variableValueStr = mappingValue.getValue(this.serializableDictionary);

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
    private static TripleComponentRole intToTripleComponentRole(int role) {
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

    // Crear una nueva instancia de SolutionMappingHDT solo con las variables especificadas
    public SolutionMappingHDT newSolutionMapping(String[] vars) {
        SolutionMappingHDT newMapping = new SolutionMappingHDT(this.serializableDictionary);
        for (String var : vars) {
            if (this.mapping.containsKey(var)) {
                newMapping.putMapping(var, this.mapping.get(var));  // Copiar la variable y su valor
            }
        }
        return newMapping;
    }

    // Proyectar solo las variables especificadas en una nueva instancia de SolutionMappingHDT
    public SolutionMappingHDT project(String[] vars) {
        return newSolutionMapping(vars);
    }
}