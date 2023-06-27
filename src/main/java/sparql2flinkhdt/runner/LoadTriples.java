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
            hdt = HDTManager.generateHDT(filePath, "", RDFNotation.parse("ntriples"), new HDTSpecification(),null);

//            HDT hdt1 = HDTManager.generateHDT(
//                    rdfInput,         // Input RDF File
//                    baseURI,          // Base URI
//                    RDFNotation.parse(inputType), // Input Type
//                    new HDTSpecification(),   // HDT Options
//                    null              // Progress Listener




        }catch (Exception e){

        }
        return hdt;
	}
}

