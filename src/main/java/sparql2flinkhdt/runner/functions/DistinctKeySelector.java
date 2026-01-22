package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

// SolutionMapping - Distinct Key Selector
public class DistinctKeySelector implements KeySelector<SolutionMappingHDT, String> {

    private String[] keys = null;

    public DistinctKeySelector(String[] keys){
        this.keys = keys;
    }

    @Override
    public String getKey(SolutionMappingHDT sm) {
        StringBuilder valueBuilder = new StringBuilder();
        int i = 0;
        for (String key : keys) {
            if(sm.getMapping().containsKey(key)) {
                MappingValue mappingValue = sm.getMapping().get(key);
                if (mappingValue != null) {
                    valueBuilder.append(mappingValue.getId());
                    if (++i < keys.length) {
                        valueBuilder.append(",");
                    }
                } else {
                    throw new IllegalArgumentException("No se encontró mapeo para la clave: " + key);
                }
            }
        }
        return valueBuilder.toString();
    }
}
