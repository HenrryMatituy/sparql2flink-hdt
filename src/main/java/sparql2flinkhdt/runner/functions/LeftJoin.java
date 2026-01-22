package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

// SolutionMapping - Flat Join Function
public class LeftJoin implements FlatJoinFunction<SolutionMappingHDT, SolutionMappingHDT, SolutionMappingHDT> {

    @Override
    public void join(SolutionMappingHDT left, SolutionMappingHDT right, Collector<SolutionMappingHDT> out) throws Exception {
        if (left == null) {
            // Si el lado izquierdo es null, no podemos hacer la unión
            System.err.println("Error en LeftJoin: El mapeo izquierdo es null.");
            return;
        }

        SolutionMappingHDT result = left.leftJoin(right);
        if (result != null) {
            out.collect(result);
        } else {
            // Si result es null, indica un problema en el método leftJoin
            System.err.println("Error en LeftJoin: El resultado de leftJoin es null.");
        }
    }
}
