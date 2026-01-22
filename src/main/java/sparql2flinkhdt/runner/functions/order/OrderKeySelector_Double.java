package sparql2flinkhdt.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Double implements KeySelector<SolutionMappingHDT, Double> {

	private SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector_Double(SerializableDictionary dictionary, String key) {
		this.dictionary = dictionary;
		this.key = key;
	}

	@Override
	public Double getKey(SolutionMappingHDT sm) {
		return Double.parseDouble(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}
}
