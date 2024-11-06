package runnertest.functions;

import org.apache.flink.api.common.functions.CrossFunction;

public class Cross implements CrossFunction<SolutionMappingHDT, SolutionMappingHDT, SolutionMappingHDT> {

    @Override
    public SolutionMappingHDT cross(SolutionMappingHDT left, SolutionMappingHDT right) {
        return left.join(right);
    }
}
