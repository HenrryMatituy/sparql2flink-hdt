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

    public CreateFlinkProgram(String flinkProgram, Path fileName){
        this.flinkProgram = flinkProgram;

        this.fileName = fileName.getFileName().toString();
        this.fileName = this.fileName.substring(0, this.fileName.indexOf('.'));
        this.fileName = this.fileName.toLowerCase();
        this.fileName = this.fileName.substring(0, 1).toUpperCase() + this.fileName.substring(1, this.fileName.length());
    }

    public void createFlinkProgram() {
        byte data[] = this.flinkProgram.getBytes();
        //RUN IDE
        Path path = Paths.get("./src/main/java/sparql2flinkhdt/out/" + this.fileName + ".java");

        //RUN Docker
//        Path path = Paths.get("../../sparql2flink/src/main/java/sparql2flinkhdt/out/" + this.fileName + ".java");
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING))) {
            out.write(data, 0, data.length);
            System.out.println("Java Program File << "+fileName+".java >> created successfully...");
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
