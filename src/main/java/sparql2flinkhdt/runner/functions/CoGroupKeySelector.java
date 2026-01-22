package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

public class CoGroupKeySelector implements KeySelector<SolutionMappingHDT, String> {

    private String[] keys = null;

    public CoGroupKeySelector(String[] keys){
        this.keys = keys;
    }

    @Override
    public String getKey(SolutionMappingHDT sm) {
        StringBuilder valueBuilder = new StringBuilder();
        int i = 0;
        for (String key : keys) {
            MappingValue mappingValue = sm.getMapping().get(key);
            if (mappingValue != null) {
                valueBuilder.append(mappingValue.getId());
            } else {
                throw new IllegalArgumentException("No se encontró mapeo para la clave: " + key);
            }
            if (++i < keys.length) {
                valueBuilder.append(",");
            }
        }
        return valueBuilder.toString();
    }
}
