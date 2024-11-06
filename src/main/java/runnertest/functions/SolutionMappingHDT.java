package runnertest.functions;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.sse.SSE;
import runnertest.SerializableDictionary;

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
        private Integer role;

        public MappingValue(Long id, Integer role) {
            this.id = id;
            this.role = role;
        }

        public Long getId() {
            return id;
        }

        public Integer getRole() {
            return role;
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
        if (var != null && val != null) {
            mapping.put(var, val);
            // Verificación adicional de serialización
            if (!(var instanceof Serializable) || !(val instanceof Serializable)) {
                System.err.println("Advertencia: Variable o valor no son serializables. Var=" + var + ", Val=" + val);
            }
        } else {
            logger.warning("Intento de agregar un mapeo con variable o valor nulo.");
        }
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
        try {
            Expr expr = SSE.parseExpr(expression);  // Parseamos la expresión

            // Creamos un Binding con las variables y sus valores
            Binding binding = BindingFactory.binding();
            for (Map.Entry<String, MappingValue> entry : mapping.entrySet()) {
                String varName = entry.getKey().startsWith("?") ? entry.getKey().substring(1) : entry.getKey();
                MappingValue value = entry.getValue();

                // Obtenemos el Node utilizando el MappingValue
                Node node = TripleIDConvert.idToString(serializableDictionary, value);

                binding = BindingFactory.binding(binding, Var.alloc(varName), node);
            }

            // Evaluamos la expresión con el binding
            NodeValue result = expr.eval(binding, null);

            return result.getBoolean();
        } catch (ExprEvalException e) {
            logger.severe("Error al evaluar la expresión: " + e.getMessage());
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            logger.severe("Error inesperado al evaluar el filtro: " + e.getMessage());
            e.printStackTrace();
            return false;
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