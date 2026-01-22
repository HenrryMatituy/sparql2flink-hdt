package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;

public class Triple2Triple implements FilterFunction<TripleID> {

    private SerializableDictionary dictionary;
    private String subject;
    private String predicate;
    private String object;

    public Triple2Triple(SerializableDictionary dictionary, String subject, String predicate, String object) {
        this.dictionary = dictionary;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    @Override
    public boolean filter(TripleID t) {
        System.out.println("Entrando al método filter de Triple2Triple");

        // Obtener los IDs actuales del triple
        long tSubject = t.getSubject();
        long tPredicate = t.getPredicate();
        long tObject = t.getObject();

        // Convertir los IDs a URIs para fines de depuración
        String uriSubject = TripleIDConvert.idToString(dictionary, tSubject, TripleComponentRole.SUBJECT);
        String uriPredicate = TripleIDConvert.idToString(dictionary, tPredicate, TripleComponentRole.PREDICATE);
        String uriObject = TripleIDConvert.idToString(dictionary, tObject, TripleComponentRole.OBJECT);

        System.out.println("Triple actual: Sujeto=" + uriSubject + ", Predicado=" + uriPredicate + ", Objeto=" + uriObject);

        // Procesar sujeto
        Long expectedSubject = null;
        if (subject != null) {
            expectedSubject = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
            if (expectedSubject == null) {
                System.out.println("Error: Sujeto no encontrado en el diccionario: " + subject);
                return false;
            }
            System.out.println("Sujeto esperado ID=" + expectedSubject + ", Sujeto actual ID=" + tSubject);
        }

        // Procesar predicado
        Long expectedPredicate = null;
        if (predicate != null) {
            expectedPredicate = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
            if (expectedPredicate == null) {
                System.out.println("Error: Predicado no encontrado en el diccionario: " + predicate);
                return false;
            }
            System.out.println("Predicado esperado ID=" + expectedPredicate + ", Predicado actual ID=" + tPredicate);
        }

        // Procesar objeto
        Long expectedObject = null;
        if (object != null) {
            expectedObject = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
            if (expectedObject == null) {
                System.out.println("Error: Objeto no encontrado en el diccionario: " + object);
                return false;
            }
            System.out.println("Objeto esperado ID=" + expectedObject + ", Objeto actual ID=" + tObject);
        }

        // Realizar las comparaciones
        boolean subjectMatches = (subject == null) || (tSubject == expectedSubject);
        boolean predicateMatches = (predicate == null) || (tPredicate == expectedPredicate);
        boolean objectMatches = (object == null) || (tObject == expectedObject);

        // Depuración detallada
        System.out.println("Comparación: Sujeto=" + subjectMatches + ", Predicado=" + predicateMatches + ", Objeto=" + objectMatches);

        // Resultado final del filtro
        return subjectMatches && predicateMatches && objectMatches;
    }
}
