package sparql2flinkhdt.runner.functions.filter;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.Expr;
import org.rdfhdt.hdt.dictionary.Dictionary;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.HashMap;

public class LessThan{

	public static boolean eval(Dictionary dictionary, E_LessThan expression, HashMap<String, Integer[]> sm) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        Boolean flag = false;
        Node value_left = null;
        Node value_right = null;

        if (arg1.isConstant() && arg2.isVariable()) {
            value_left = arg1.getConstant().getNode();
            value_right = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg2.toString()));
        } else if (arg1.isVariable() && arg2.isConstant()) {
            value_left = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg1.toString()));
            value_right = arg2.getConstant().getNode();
        } else if(arg1.isVariable() && arg2.isVariable()) {
            value_left = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg1.toString()));
            value_right = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg2.toString()));
        }

		if (value_left.getLiteralDatatype().getJavaClass().equals(BigDecimal.class)) {
			if (Double.parseDouble(value_left.getLiteralValue().toString()) < Double.parseDouble(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- BigDecimal");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(BigInteger.class)) {
			if (Integer.parseInt(value_left.getLiteralValue().toString()) < Integer.parseInt(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- BigDecimal");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(Float.class)) {
			if (Float.parseFloat(value_left.getLiteralValue().toString()) < Float.parseFloat(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- BigDecimal");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(Double.class)) {
			if (Double.parseDouble(value_left.getLiteralValue().toString()) < Double.parseDouble(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- Double");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(Integer.class)) {
			if (Integer.parseInt(value_left.getLiteralValue().toString()) < Integer.parseInt(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- Integer");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(Long.class)) {
			if (Long.parseLong(value_left.getLiteralValue().toString()) < Long.parseLong(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- Long");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(Short.class)) {
			if (Short.parseShort(value_left.getLiteralValue().toString()) < Short.parseShort(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- Short");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(Byte.class)) {
			if (Byte.parseByte(value_left.getLiteralValue().toString()) < Byte.parseByte(value_right.getLiteralValue().toString())) {
				//System.out.println("--- LessThan --- Short");
				flag = true;
			}
		} else if (value_left.getLiteralDatatype().getJavaClass().equals(XSDDateTime.class)) {
			Timestamp timestampLeft = Timestamp.valueOf(value_left.getLiteralValue().toString().replace("T", " "));
			Timestamp timestampRight = Timestamp.valueOf(value_right.getLiteralValue().toString().replace("T", " "));
			long leftTime = timestampLeft.getTime();
			long rightTime = timestampRight.getTime();

			if (leftTime < rightTime) {
				//System.out.println("--- LessThan --- XSDDateTime");
				flag = true;
			}
		} else {
			System.out.println("--- getJavaClass --- " + value_left.getLiteralDatatype().getJavaClass());
		}

		return flag;
	}
}
