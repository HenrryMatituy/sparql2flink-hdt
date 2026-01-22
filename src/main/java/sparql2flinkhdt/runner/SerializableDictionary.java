package sparql2flinkhdt.runner;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SerializableDictionary implements Serializable {
    private static final long serialVersionUID = 1L;

    // Mapas para almacenar las relaciones entre URIs y IDs
    private final Map<String, Long> subjectMap = new HashMap<>();
    private final Map<String, Long> predicateMap = new HashMap<>();
    private final Map<String, Long> objectMap = new HashMap<>();
    private final Map<Long, String> reverseSubjectMap = new HashMap<>();
    private final Map<Long, String> reversePredicateMap = new HashMap<>();
    private final Map<Long, String> reverseObjectMap = new HashMap<>();

    private static final Logger logger = Logger.getLogger(SerializableDictionary.class.getName());

    // Cargar las secciones del diccionario (sujetos, predicados, objetos) desde HDT
    public void loadFromHDTDictionary(Dictionary dictionary) {
        loadSection(dictionary, TripleComponentRole.SUBJECT);
        loadSection(dictionary, TripleComponentRole.PREDICATE);
        loadSection(dictionary, TripleComponentRole.OBJECT);
        // Verificar si el predicado mbox se cargó correctamente
        Long mboxID = predicateMap.get("http://xmlns.com/foaf/0.1/mbox");
        System.out.println("Verificando mbox en diccionario: " + mboxID);
    }

    // Método para cargar cada sección del diccionario (sujeto, predicado, objeto)
    private void loadSection(Dictionary dictionary, TripleComponentRole role) {
        long numEntries = 0;
        Map<String, Long> map = null;
        Map<Long, String> reverseMap = null;

        // Identificar el rol y asignar los mapas correspondientes
        switch (role) {
            case SUBJECT:
                numEntries = dictionary.getNsubjects();
                map = subjectMap;
                reverseMap = reverseSubjectMap;
                System.out.println("Cargando SUBJECTs (SERIALIZABLEDICTIONARY): " + numEntries);
                break;
            case PREDICATE:
                numEntries = dictionary.getNpredicates();
                map = predicateMap;
                reverseMap = reversePredicateMap;
                System.out.println("Cargando PREDICATEs (SERIALIZABLEDICTIONARY): " + numEntries);
                break;
            case OBJECT:
                numEntries = dictionary.getNobjects();
                map = objectMap;
                reverseMap = reverseObjectMap;
                System.out.println("Cargando OBJECTs (SERIALIZABLEDICTIONARY): " + numEntries);
                break;
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }

        // Procesar cada entrada de la sección
        for (long i = 1; i <= numEntries; i++) {
            CharSequence result = dictionary.idToString(i, role);
            String uri = (result != null) ? result.toString() : null;

            if (uri != null && !uri.isEmpty()) {
                map.put(uri, i);
                reverseMap.put(i, uri);
                System.out.println(String.format("ID=%d, Role=%s, URI=%s", i, role, uri));
            } else {
                System.out.println(String.format("URI vacía o nula para el ID=%d, Role=%s", i, role));
            }
        }
    }

    // Método para convertir una URI a su ID correspondiente según el rol (sujeto, predicado, objeto)
    public Long stringToID(String value, TripleComponentRole role) {
        Long id = null;
        switch (role) {
            case SUBJECT:
                id = subjectMap.get(value);
                break;
            case PREDICATE:
                id = predicateMap.get(value);
                if (id == null) {
                    System.out.println("Error: El predicado no se encuentra en el mapa (SERIALIZABLEDICTIONARY STRINGTOID): " + value);
                } else {
                    System.out.println("Predicado encontrado en el mapa (SERIALIZABLEDICTIONARY STRINGTOID): " + value + " -> ID: " + id);
                }
                break;
            case OBJECT:
                id = objectMap.get(value);
                break;
        }

        if (id == null) {
            System.out.println("Error: No se encontró el ID para value: " + value + ", role: " + role + " (SERIALIZABLEDICTIONARY STRINGTOID)");
            return null;
        }
        return id;
    }

    // Método para convertir un ID a su URI correspondiente según el rol (sujeto, predicado, objeto)
    public String idToString(long id, TripleComponentRole role) {
        String value = null;
        switch (role) {
            case SUBJECT:
                value = reverseSubjectMap.get(id);
                break;
            case PREDICATE:
                value = reversePredicateMap.get(id);
                System.out.println("Asignando ID para predicado URI (SERIALIZABLEDICTIONARY): " + value + " -> ID: " + id);
                break;
            case OBJECT:
                value = reverseObjectMap.get(id);
                break;
        }

        if (value == null) {
            System.out.println("idToString: No value found for id (SERIALIZABLEDICTIONARY): " + id + ", role: " + role);
            return null;
        }
        System.out.println(String.format("idToString: id=%d, role=%s, value=%s (SERIALIZABLEDICTIONARY)", id, role, value));
        return value;
    }

    public void printPredicateMappings() {
        System.out.println("IDs de predicados en el diccionario:");
        for (Map.Entry<String, Long> entry : this.predicateMap.entrySet()) {
            System.out.println("URI: " + entry.getKey() + " -> ID: " + entry.getValue());
        }
    }
}
