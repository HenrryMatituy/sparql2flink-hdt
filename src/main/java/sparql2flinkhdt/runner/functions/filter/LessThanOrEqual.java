package sparql2flinkhdt.runner.functions.filter;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.*;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.HashMap;

public class LessThanOrEqual {

	public static boolean eval(SerializableDictionary dictionary, E_LessThanOrEqual expression, HashMap<String, MappingValue> sm) {
		Expr arg1 = expression.getArg1();
		Expr arg2 = expression.getArg2();

		boolean flag = false;
		Node value_left = null;
		Node value_right = null;

		try {
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
			} else if (arg1.isConstant() && arg2.isConstant()) {
				value_left = arg1.getConstant().getNode();
				value_right = arg2.getConstant().getNode();
			} else {
				throw new IllegalArgumentException("Los argumentos no son constantes ni variables reconocidas.");
			}

			// Asegurarse de que ambos valores no sean nulos
			if (value_left == null || value_right == null) {
				throw new IllegalArgumentException("No se pudieron obtener los valores para la comparación.");
			}

			// Realizar la comparación según el tipo de dato
			if (value_left.isLiteral() && value_right.isLiteral()) {
				flag = compareLiterals(value_left, value_right);
			} else {
				throw new IllegalArgumentException("Los valores no son literales y no se pueden comparar.");
			}

		} catch (Exception e) {
			System.err.println("Error en LessThanOrEqual.eval: " + e.getMessage());
			e.printStackTrace();
			flag = false;
		}

		return flag;
	}

	private static boolean compareLiterals(Node value_left, Node value_right) {
		Object leftValue = value_left.getLiteralValue();
		Object rightValue = value_right.getLiteralValue();

		if (leftValue instanceof Number && rightValue instanceof Number) {
			// Convertir a Double para comparación general
			double leftNum = ((Number) leftValue).doubleValue();
			double rightNum = ((Number) rightValue).doubleValue();
			return leftNum <= rightNum;
		} else if (leftValue instanceof XSDDateTime && rightValue instanceof XSDDateTime) {
			XSDDateTime leftDateTime = (XSDDateTime) leftValue;
			XSDDateTime rightDateTime = (XSDDateTime) rightValue;
			return leftDateTime.compareTo(rightDateTime) <= 0;
		} else if (leftValue instanceof String && rightValue instanceof String) {
			return ((String) leftValue).compareTo((String) rightValue) <= 0;
		} else {
			// Agregar más casos si es necesario
			System.err.println("Tipos de datos no compatibles para comparación: " + leftValue.getClass() + ", " + rightValue.getClass());
			return false;
		}
	}
}
