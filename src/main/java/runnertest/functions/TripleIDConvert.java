package runnertest.functions;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import runnertest.SerializableDictionary;

import java.util.logging.Logger;

public class TripleIDConvert {

    private static final Logger logger = Logger.getLogger(TripleIDConvert.class.getName());
//    public static Node idToString(SerializableDictionary dictionary, SolutionMappingHDT.MappingValue mappingValue) {
//        if (mappingValue == null) {
//            logger.severe("idToString: mappingValue is null (TRIPLEIDCONVERT)");
//            return null;
//        }
//
//        Long id = mappingValue.getId();
//        Integer roleCode = mappingValue.getRole();
//
//        // Depuración: imprimir ID y rol antes de convertirlo
//        System.out.println("Procesando ID: " + id + " con rol: " + getRole(roleCode));
//
//        String uri = dictionary.idToString(id, getRole(roleCode));
//        if (uri == null) {
//            logger.severe("idToString: URI is null for id (TRIPLEIDCONVERT): " + id + ", role: " + getRole(roleCode));
//            return null;
//        }
//
//        // Si es un literal (comienza con comillas dobles)
//        if (roleCode == 3 && uri.startsWith("\"")) {
//            System.out.println("Literal detectado en idToString: " + uri);  // Depuración
//            return createLiteralNode(uri);
//        }
//
//        // Si no es un literal, crear un nodo URI
//        System.out.println("Nodo URI procesado: " + uri);  // Depuración
//        return NodeFactory.createURI(uri);
//    }



//    Habilitar después de prueba

    // Método para convertir un MappingValue en un Node de Jena (URI o literal)
    public static Node idToString(SerializableDictionary dictionary, SolutionMappingHDT.MappingValue mappingValue) {
        if (mappingValue == null) {
            logger.severe("idToString: mappingValue is null (TRIPLEIDCONVERT)");
            return null;
        }

        Long id = mappingValue.getId();
        Integer roleCode = mappingValue.getRole();

        String uri = dictionary.idToString(id, getRole(roleCode));
        if (uri == null) {
            logger.severe("idToString: URI is null for id (TRIPLEIDCONVERT): " + id + ", role: " + getRole(roleCode));
            return null;
        }

        // Si es un literal (comienza con comillas dobles)
        if (roleCode == 3 && uri.startsWith("\"")) {
            return createLiteralNode(uri);
        }

        // Si no es un literal, crear un nodo URI
        return NodeFactory.createURI(uri);
    }

    // Método auxiliar para convertir una cadena y un rol en un ID
    public static Long stringToID(SerializableDictionary dictionary, String element, TripleComponentRole role) {
        Long id = dictionary.stringToID(element, role);
        if (id == null) {
            logger.severe("stringToID: No ID found for element (TRIPLEIDCONVERT): " + element + " with role: " + role);
            return null;
        }
        return id;
    }

    // Resto de los métodos...

    // Método auxiliar para convertir un literal con tipo XSD en un nodo
    private static Node createLiteralNode(String object) {
        int start = object.indexOf("\"") + 1;
        int end = object.lastIndexOf("\"");

        // Agregar un mensaje de depuración
        System.out.println("Procesando literal: " + object);

        if (start <= 0 || end <= 0 || start >= end) {
            logger.severe("createLiteralNode: Invalid literal format (TRIPLEIDCONVERT): " + object);
            return null;
        }

        String value = object.substring(start, end);

        // Si el literal tiene un tipo (e.g., "value"^^<type>)
        if (object.contains("^^")) {
            String type = object.substring(object.indexOf("^^") + 2).trim();
            if (type.startsWith("<") && type.endsWith(">")) {
                type = type.substring(1, type.length() - 1);
            }
            RDFDatatype rdfDatatype = getXSDDatatype(type);
            System.out.println("Literal procesado con tipo: " + type + ", valor: " + value);  // Depuración
            return NodeFactory.createLiteral(value, rdfDatatype);
        }

        // Si no tiene tipo, devolver un literal simple
        System.out.println("Literal procesado: " + value);  // Depuración
        return NodeFactory.createLiteral(value);
    }

// Restarurar después de la prueba
//    private static Node createLiteralNode(String object) {
//        int start = object.indexOf("\"") + 1;
//        int end = object.lastIndexOf("\"");
//        if (start <= 0 || end <= 0 || start >= end) {
//            logger.severe("createLiteralNode: Invalid literal format (TRIPLEIDCONVERT): " + object);
//            return null;
//        }
//
//        String value = object.substring(start, end);
//
//        // Si el literal tiene un tipo (e.g., "value"^^<type>)
//        if (object.contains("^^")) {
//            String type = object.substring(object.indexOf("^^") + 2).trim();
//            if (type.startsWith("<") && type.endsWith(">")) {
//                type = type.substring(1, type.length() - 1);
//            }
//            RDFDatatype rdfDatatype = getXSDDatatype(type);
//            return NodeFactory.createLiteral(value, rdfDatatype);
//        }
//
//        // Si no tiene tipo, devolver un literal simple
//        return NodeFactory.createLiteral(value);
//    }

    // Método auxiliar para devolver el tipo de XSD correcto
    private static RDFDatatype getXSDDatatype(String datatypeURI) {
        RDFDatatype rdfDatatype = TypeMapper.getInstance().getTypeByName(datatypeURI);
        if (rdfDatatype == null) {
            logger.warning("getXSDDatatype: Unknown datatype URI (TRIPLEIDCONVERT): " + datatypeURI);
            rdfDatatype = TypeMapper.getInstance().getTypeByName("http://www.w3.org/2001/XMLSchema#string");  // Valor por defecto
        }
        return rdfDatatype;
    }

    // Método para obtener el rol a partir del código
    private static TripleComponentRole getRole(Integer roleCode) {
        if (roleCode == null) {
            logger.severe("getRole: Null role code (TRIPLEIDCONVERT)");
            return null;
        }

        switch (roleCode) {
            case 1:
                return TripleComponentRole.SUBJECT;
            case 2:
                return TripleComponentRole.PREDICATE;
            case 3:
                return TripleComponentRole.OBJECT;
            default:
                throw new IllegalArgumentException("Unknown role code (TRIPLEIDCONVERT): " + roleCode);
        }
    }

    // Método idToStringFilter para convertir MappingValue a Node con manejo de literales
    public static Node idToStringFilter(SerializableDictionary dictionary, SolutionMappingHDT.MappingValue mappingValue) {
        return idToString(dictionary, mappingValue);
    }

    // Método para convertir un ID y un rol en una cadena (URI o literal)
    public static String idToString(SerializableDictionary dictionary, long id, TripleComponentRole role) {
        return dictionary.idToString(id, role);
    }
}
