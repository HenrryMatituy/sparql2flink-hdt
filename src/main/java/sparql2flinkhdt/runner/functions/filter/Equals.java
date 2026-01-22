package sparql2flinkhdt.runner.functions.filter;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.*;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.TripleIDConvert;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

import java.util.HashMap;

public class Equals {

	public static boolean eval(SerializableDictionary dictionary, E_Equals expression, HashMap<String, MappingValue> sm) {
		Expr arg1 = expression.getArg1();
		Expr arg2 = expression.getArg2();

		Node value_left = null;
		Node value_right = null;

		if (arg1.isConstant() && arg2.isVariable()) {
			value_left = arg1.getConstant().getNode();
			MappingValue mappingValue = sm.get(arg2.toString());
			if (mappingValue != null) {
				value_right = TripleIDConvert.idToStringFilter(dictionary, mappingValue);
			} else {
				throw new IllegalArgumentException("No se encontró mapeo para la variable: " + arg2.toString());
			}
		} else if (arg1.isVariable() && arg2.isConstant()) {
			MappingValue mappingValue = sm.get(arg1.toString());
			if (mappingValue != null) {
				value_left = TripleIDConvert.idToStringFilter(dictionary, mappingValue);
			} else {
				throw new IllegalArgumentException("No se encontró mapeo para la variable: " + arg1.toString());
			}
			value_right = arg2.getConstant().getNode();
		} else if (arg1.isVariable() && arg2.isVariable()) {
			MappingValue mappingValue1 = sm.get(arg1.toString());
			MappingValue mappingValue2 = sm.get(arg2.toString());
			if (mappingValue1 != null && mappingValue2 != null) {
				value_left = TripleIDConvert.idToStringFilter(dictionary, mappingValue1);
				value_right = TripleIDConvert.idToStringFilter(dictionary, mappingValue2);
			} else {
				throw new IllegalArgumentException("No se encontraron mapeos para las variables: " + arg1.toString() + ", " + arg2.toString());
			}
		}

		if (value_left != null && value_right != null) {
			return value_left.equals(value_right);
		} else {
			throw new IllegalArgumentException("No se pudieron obtener los valores para la comparación.");
		}
	}
}
