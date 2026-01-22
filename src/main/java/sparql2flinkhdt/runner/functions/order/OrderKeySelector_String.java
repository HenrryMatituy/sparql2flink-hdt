package sparql2flinkhdt.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_String implements KeySelector<SolutionMappingHDT, String> {

	private SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector_String(SerializableDictionary dictionary, String key) {
		this.dictionary = dictionary;
		this.key = key;
	}

	@Override
	public String getKey(SolutionMappingHDT sm) {
		String value = "";
		Node node = TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key));
		if (node.isLiteral()) {
			value = node.getLiteralValue().toString();
		} else if (node.isURI()) {
			value = node.toString();
		}
		return value;
	}
}
