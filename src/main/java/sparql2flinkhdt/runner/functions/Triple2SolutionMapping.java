package sparql2flinkhdt.runner.functions;


import org.apache.flink.api.common.functions.MapFunction;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

public class Triple2SolutionMapping implements MapFunction<TripleID, SolutionMappingHDT> {

    private String var_s, var_p, var_o;
    private SerializableDictionary serializableDictionary;

    public Triple2SolutionMapping(String s, String p, String o, SerializableDictionary serializableDictionary) {
        this.var_s = s;
        this.var_p = p;
        this.var_o = o;
        this.serializableDictionary = serializableDictionary;
    }

    @Override
    public SolutionMappingHDT map(TripleID t) {
        SolutionMappingHDT sm = new SolutionMappingHDT(serializableDictionary);
        try {

            if (var_s != null && var_p == null && var_o == null) {
                System.out.println("Mapeando solo sujeto");
                sm.putMapping(var_s, new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
            } else if (var_s != null && var_p != null && var_o == null) {
                System.out.println("Mapeando sujeto y predicado");
                sm.putMapping(var_s, new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
                sm.putMapping(var_p, new SolutionMappingHDT.MappingValue(t.getPredicate(), 2));
            } else if (var_s != null && var_p == null && var_o != null) {
                System.out.println("Mapeando sujeto y objeto");
                sm.putMapping(var_s, new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
                sm.putMapping(var_o, new SolutionMappingHDT.MappingValue(t.getObject(), 3));
            } else if (var_s == null && var_p != null && var_o == null) {
                System.out.println("Mapeando solo predicado");
                sm.putMapping(var_p, new SolutionMappingHDT.MappingValue(t.getPredicate(), 2));
            } else if (var_s == null && var_p != null && var_o != null) {
                System.out.println("Mapeando predicado y objeto");
                sm.putMapping(var_p, new SolutionMappingHDT.MappingValue(t.getPredicate(), 2));
                sm.putMapping(var_o, new SolutionMappingHDT.MappingValue(t.getObject(), 3));
            } else if (var_s == null && var_p == null && var_o != null) {
                System.out.println("Mapeando solo objeto");
                sm.putMapping(var_o, new SolutionMappingHDT.MappingValue(t.getObject(), 3));
            } else {
                System.out.println("Mapeando sujeto, predicado y objeto");
                if (var_s != null) {
                    sm.putMapping(var_s, new SolutionMappingHDT.MappingValue(t.getSubject(), 1));
                }
                if (var_p != null) {
                    sm.putMapping(var_p, new SolutionMappingHDT.MappingValue(t.getPredicate(), 2));
                }
                if (var_o != null) {
                    sm.putMapping(var_o, new SolutionMappingHDT.MappingValue(t.getObject(), 3));
                }
            }
            System.out.println("Mapeo completado: " + sm.getMapping());
        } catch (Exception e) {
            System.err.println("Excepción en el método map: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        return sm;
    }
}
