package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

// SolutionMapping to Integer - Map Function
public class SolutionMapping2Node implements MapFunction<SolutionMappingHDT, Integer> {

    private String label = null;

    public SolutionMapping2Node(String label){
        this.label = label;
    }

    @Override
    public Integer map(SolutionMappingHDT sm){
        MappingValue mappingValue = sm.getMapping().get(label);
        if (mappingValue != null) {
            return mappingValue.getId().intValue();
        } else {
            throw new IllegalArgumentException("No se encontró mapeo para la etiqueta: " + label);
        }
    }
}
