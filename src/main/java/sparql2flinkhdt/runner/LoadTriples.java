package sparql2flinkhdt.runner;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;


public class LoadTriples {
	public static HDT fromDataset(ExecutionEnvironment environment, String filePath) {
	    Preconditions.checkNotNull(filePath, "The file path may not be null...");
        HDT hdt = null;

        try {
            String datasetFilePath = System.getProperty("user.dir") + "/examples/dataset.nt";
            System.out.println("Ruta completa del archivo: " + datasetFilePath);

            String baseURI = "file://" + System.getProperty("user.dir") + "/examples/dataset.nt";
            hdt = HDTManager.generateHDT(filePath, baseURI, RDFNotation.parse("ntriples"), new HDTSpecification(),null);

        }catch (Exception e){

            System.err.println("Error al generar el objeto HDT Henrry: " + e.getMessage());
            e.printStackTrace();
        }
        return hdt;
	}


}

