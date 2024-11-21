package sparql2flinkhdt.mapper;

import org.apache.jena.graph.Triple;

import java.util.ArrayList;
import java.util.List;

public class ConvertTriplePatternGroup {

    public ConvertTriplePatternGroup() {
    }

    public static String joinSolutionMapping(int indice_sm_join, int indice_sm_left, int indice_sm_right) {
        StringBuilder sm = new StringBuilder();
        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);

        if (listKeys.size() > 0) {
            String keys = JoinKeys.keys(listKeys);
            sm.append("\t\tDataSet<SolutionMappingHDT> sm").append(indice_sm_join)
                    .append(" = sm").append(indice_sm_left).append(".join(sm").append(indice_sm_right).append(")\n")
                    .append("\t\t\t.where(new JoinKeySelector(new String[]{").append(keys).append("}))\n")
                    .append("\t\t\t.equalTo(new JoinKeySelector(new String[]{").append(keys).append("}))\n")
                    .append("\t\t\t.with(new Join());\n\n");
        } else {
            sm.append("\t\tDataSet<SolutionMappingHDT> sm").append(indice_sm_join)
                    .append(" = sm").append(indice_sm_left).append(".cross(sm").append(indice_sm_right).append(")\n")
                    .append("\t\t\t.with(new Cross());\n\n");
        }

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
        return sm.toString();
    }

    public static String convertTPG(List<Triple> listTriplePatterns, int indiceLTP, int count, int indiceSM, String bgp) {
        StringBuilder bgpBuilder = new StringBuilder(bgp);

        if (indiceLTP >= listTriplePatterns.size() && count == 1) {
            return bgpBuilder.toString();
        } else {
            if (count == 2) {
                bgpBuilder.append(joinSolutionMapping(indiceSM, indiceSM - 2, indiceSM - 1));
                count = 1;
            } else {
                bgpBuilder.append("\t\tDataSet<SolutionMappingHDT> sm").append(indiceSM)
                        .append(" = dataset\n")
                        .append(ConvertTriplePattern.convert(listTriplePatterns.get(indiceLTP), indiceSM));
                indiceLTP += 1;
                count += 1;
            }
            return convertTPG(listTriplePatterns, indiceLTP, count, SolutionMapping.getIndice(), bgpBuilder.toString());
        }
    }

    public static String convert(List<Triple> listTriplePatterns) {
        return convertTPG(listTriplePatterns, 0, 0, SolutionMapping.getIndice(), "");
    }
}
