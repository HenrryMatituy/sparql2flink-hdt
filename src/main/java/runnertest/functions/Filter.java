package runnertest.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import runnertest.SerializableDictionary;

// SolutionMapping to SolutionMapping - Filter Function
public class Filter implements FilterFunction<SolutionMappingHDT> {

    private SerializableDictionary dictionary;
    private String expression;

    public Filter(SerializableDictionary dictionary, String expression) {
        this.dictionary = dictionary;
        this.expression = expression;
    }

    @Override
    public boolean filter(SolutionMappingHDT sm) {
        // Asegurarse de que 'sm' tiene el 'dictionary' configurado
        if (sm.getSerializableDictionary() == null) {
            sm.setSerializableDictionary(this.dictionary);
        }
        return sm.filter(expression);
    }
}
