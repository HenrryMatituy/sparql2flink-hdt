package sparql2flinkhdt.runner;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.thrift.TSerializable;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class LoadTriples implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTriples.class);
    public static HDT fromDataset(ExecutionEnvironment environment, String filePath) {
        Preconditions.checkNotNull(filePath, "The file path may not be null...");
        HDT hdt = null;

        try {
            LOG.info("Intentando generar HDT desde el archivo: {}", filePath);
            hdt = HDTManager.generateHDT(filePath, "http://example.org/baseURI", RDFNotation.parse("ntriples"), new HDTSpecification(), null);
            LOG.info("Generación de HDT exitosa.");

        } catch (Exception e) {
            LOG.error("Error al generar HDT desde el archivo: {}", filePath, e);
        }

        if (hdt == null) {
            throw new RuntimeException("Error: no se pudo generar HDT desde el archivo: " + filePath);
        }
        return hdt;
    }

}

