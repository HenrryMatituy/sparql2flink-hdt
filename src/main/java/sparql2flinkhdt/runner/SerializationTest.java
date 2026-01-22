import sparql2flinkhdt.runner.SerializableDictionary;

import java.io.*;

public class SerializationTest {

    public static void main(String[] args) {
        // Crea una instancia de SerializableDictionary
        SerializableDictionary dictionary = new SerializableDictionary();

        // Prueba de serialización
        try {
            // Serialización
            FileOutputStream fos = new FileOutputStream("dictionary_serialized.obj");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(dictionary);
            oos.close();
            fos.close();
            System.out.println("Serialización exitosa.");

            // Deserialización
            FileInputStream fis = new FileInputStream("dictionary_serialized.obj");
            ObjectInputStream ois = new ObjectInputStream(fis);
            SerializableDictionary deserializedDictionary = (SerializableDictionary) ois.readObject();
            ois.close();
            fis.close();
            System.out.println("Deserialización exitosa.");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
