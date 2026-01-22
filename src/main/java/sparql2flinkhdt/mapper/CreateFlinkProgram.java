package sparql2flinkhdt.mapper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class CreateFlinkProgram {

    private String flinkProgram;
    private String fileName;
    private String className;

    public CreateFlinkProgram(String flinkProgram, Path filePath) {
        this.flinkProgram = flinkProgram;
        String fileNameWithExtension = filePath.getFileName().toString();
        this.fileName = fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'));
        this.className = capitalize(this.fileName);
    }

    private String capitalize(String name) {
        if (name == null || name.isEmpty()) return name;
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    public void createFlinkProgram() {
        String fullProgram = flinkProgram;
        Path path = Paths.get("./src/main/java/sparql2flinkhdt/out/" + className + ".java");

        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING))) {
            out.write(fullProgram.getBytes());
            System.out.println("Java Program File << " + className + ".java >> created successfully...");
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
