package runnertest.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import runnertest.SerializableDictionary;
import runnertest.functions.SolutionMappingHDT;
import runnertest.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Integer implements KeySelector<SolutionMappingHDT, Integer> {

	private SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector_Integer(SerializableDictionary dictionary, String key) {
		this.dictionary = dictionary;
		this.key = key;
	}

	@Override
	public Integer getKey(SolutionMappingHDT sm) {
		return Integer.parseInt(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}
}
