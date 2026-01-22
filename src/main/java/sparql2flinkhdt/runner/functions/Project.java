package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;

// SolutionMapping to SolutionMapping - Map Function
public class Project implements MapFunction<SolutionMappingHDT, SolutionMappingHDT> {

    private String[] vars;

    public Project(String[] vars) {
        if (vars == null || vars.length == 0) {
            throw new IllegalArgumentException("La lista de variables para proyectar no puede ser null o vacía.");
        }
        this.vars = vars;
    }

    @Override
    public SolutionMappingHDT map(SolutionMappingHDT sm) {
        if (sm == null) {
            System.err.println("Error en Project: El SolutionMappingHDT es null.");
            return null;
        }
        SolutionMappingHDT projectedSM = sm.project(vars);
        if (projectedSM == null) {
            System.err.println("Error en Project: El resultado de la proyección es null.");
        }
        return projectedSM;
    }
}
