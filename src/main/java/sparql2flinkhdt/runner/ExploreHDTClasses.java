package sparql2flinkhdt.runner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ExploreHDTClasses {
    public static void main(String[] args) {
        try {
            Class<?> dictionaryClass = Class.forName("org.rdfhdt.hdt.dictionary.Dictionary");
            System.out.println("Methods in Dictionary class:");
            for (Method method : dictionaryClass.getDeclaredMethods()) {
                System.out.println(method.getName());
            }

            System.out.println("\nFields in Dictionary class:");
            for (Field field : dictionaryClass.getDeclaredFields()) {
                System.out.println(field.getName());
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
