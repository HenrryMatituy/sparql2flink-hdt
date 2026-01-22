package sparql2flinkhdt.runner.functions;

import org.apache.jena.sparql.expr.*;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.filter.*;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

import java.util.HashMap;

// SolutionMapping - Filter Function
public class FilterConvert {

    public static boolean convert(SerializableDictionary dictionary, E_Equals expression, HashMap<String, MappingValue> hm) {
        return Equals.eval(dictionary, expression, hm);
    }

    public static boolean convert(SerializableDictionary dictionary, E_NotEquals expression, HashMap<String, MappingValue> hm) {
        return NotEquals.eval(dictionary, expression, hm);
    }

    public static boolean convert(SerializableDictionary dictionary, E_GreaterThanOrEqual expression, HashMap<String, MappingValue> hm) {
        return GreaterThanOrEqual.eval(dictionary, expression, hm);
    }

    public static boolean convert(SerializableDictionary dictionary, E_LessThanOrEqual expression, HashMap<String, MappingValue> hm) {
        return LessThanOrEqual.eval(dictionary, expression, hm);
    }

    public static boolean convert(SerializableDictionary dictionary, E_GreaterThan expression, HashMap<String, MappingValue> hm) {
        return GreaterThan.eval(dictionary, expression, hm);
    }

    public static boolean convert(SerializableDictionary dictionary, E_LessThan expression, HashMap<String, MappingValue> hm) {
        return LessThan.eval(dictionary, expression, hm);
    }

    public static boolean convert(SerializableDictionary dictionary, E_LogicalAnd expression, HashMap<String, MappingValue> hm) {
        return (convert(dictionary, expression.getArg1(), hm) && convert(dictionary, expression.getArg2(), hm));
    }

    public static boolean convert(SerializableDictionary dictionary, E_LogicalOr expression, HashMap<String, MappingValue> hm) {
        return (convert(dictionary, expression.getArg1(), hm) || convert(dictionary, expression.getArg2(), hm));
    }

    public static boolean convert(SerializableDictionary dictionary, Expr expression, HashMap<String, MappingValue> hm) {
        if (expression instanceof E_Equals) return convert(dictionary, (E_Equals) expression, hm);
        if (expression instanceof E_NotEquals) return convert(dictionary, (E_NotEquals) expression, hm);
        if (expression instanceof E_LessThan) return convert(dictionary, (E_LessThan) expression, hm);
        if (expression instanceof E_LessThanOrEqual) return convert(dictionary, (E_LessThanOrEqual) expression, hm);
        if (expression instanceof E_GreaterThan) return convert(dictionary, (E_GreaterThan) expression, hm);
        if (expression instanceof E_GreaterThanOrEqual) return convert(dictionary, (E_GreaterThanOrEqual) expression, hm);
        if (expression instanceof E_LogicalAnd) return convert(dictionary, (E_LogicalAnd) expression, hm);
        if (expression instanceof E_LogicalOr) return convert(dictionary, (E_LogicalOr) expression, hm);
        throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
    }
}
