package sparql2flinkhdt.runner.functions.filter;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.*;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

import java.util.HashMap;

public class NotEquals {

    public static boolean eval(SerializableDictionary dictionary, E_NotEquals expression, HashMap<String, MappingValue> sm) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        boolean flag = false;
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
                throw new IllegalArgumentException("No se encontró mapeo para las variables: " + arg1.toString() + ", " + arg2.toString());
            }
        }

        if (value_left != null && value_right != null) {
            if (value_left.isLiteral() && value_right.isLiteral()) {
                flag = !value_left.getLiteralValue().toString().equals(value_right.getLiteralValue().toString());
            } else if (value_left.isURI() && value_right.isURI()) {
                flag = !value_left.getURI().equals(value_right.getURI());
            } else {
                // Comparación entre tipos diferentes o nulos
                flag = true;
            }
        } else {
            throw new IllegalArgumentException("No se pudieron obtener los valores para la comparación.");
        }

        return flag;
    }
}
