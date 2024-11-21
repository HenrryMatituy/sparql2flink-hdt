package sparql2flinkhdt.mapper;

import org.apache.jena.graph.Triple;
import sparql2flinkhdt.runner.SerializableDictionary;

import java.util.ArrayList;
import java.util.List;

public class ConvertTriplePatternGroup {

    public ConvertTriplePatternGroup() { }

    public static String joinSolutionMapping(int indice_sm_join, int indice_sm_left, int indice_sm_right) {
        String sm = "";
        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);
        if (listKeys.size() > 0) {
            String keys = JoinKeys.keys(listKeys);
            sm = "\t\tDataSet<SolutionMappingHDT> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.where(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                    "\t\t\t.with(new Join());" +
                    "\n\n";
        } else {
            sm = "\t\tDataSet<SolutionMappingHDT> sm" + indice_sm_join + " = sm" + indice_sm_left + ".cross(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.with(new Cross());" +
                    "\n\n";
        }
        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
        return sm;
    }

    public static String convertTPG(List<Triple> listTriplePatterns, int indiceLTP, int count, int indiceSM, String bgp, SerializableDictionary dictionary) {
        if (indiceLTP >= listTriplePatterns.size() && count == 1) {
            return bgp;
        } else {
            if (count == 2) {
                bgp += joinSolutionMapping(indiceSM, indiceSM - 2, indiceSM - 1);
                count = 1;
            } else {
                bgp += "\t\tDataSet<SolutionMappingHDT> sm" + indiceSM + " = dataset\n" +
                        ConvertTriplePattern.convert(listTriplePatterns.get(indiceLTP), indiceSM, dictionary);
                indiceLTP += 1;
                count += 1;
            }
            return convertTPG(listTriplePatterns, indiceLTP, count, SolutionMapping.getIndice(), bgp, dictionary);
        }
    }

    public static String convert(List<Triple> listTriplePatterns, SerializableDictionary dictionary) {
        return convertTPG(listTriplePatterns, 0, 0, SolutionMapping.getIndice(), "", dictionary);
    }
}
