package runnertest.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import runnertest.SerializableDictionary;
import runnertest.functions.SolutionMappingHDT;
import runnertest.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Float implements KeySelector<SolutionMappingHDT, Float> {

	private SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector_Float(SerializableDictionary dictionary, String key) {
		this.dictionary = dictionary;
		this.key = key;
	}

	@Override
	public Float getKey(SolutionMappingHDT sm) {
		return Float.parseFloat(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}
}
