package runnertest;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;
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
            // Log para verificar la ruta del archivo
            LOG.info("Intentando generar HDT desde el archivo como antes: {}", filePath);

            // Generar el HDT
//          Flink 1.10.0
//            hdt = HDTManager.generateHDT(filePath, "http://example.org/baseURI", RDFNotation.parse("ntriples"),new HDTSpecification(), null);
//Flink 1.15.0 -->
            //            hdt = HDTManager.generateHDT(filePath, "", RDFNotation.parse("ntriples"), new HDTSpecification(),null);

//            Nueva versión
            hdt = HDTManager.generateHDT(filePath, "", RDFNotation.parse("ntriples"),new HDTSpecification(), null);

            // Log para verificar que la generación fue exitosa
            if (hdt != null) {
//                LOG.info("Generación de HDT exitosa. Número de triples: {}", hdt.getTriples().estimatedNumResults());
                LOG.info("Número de sujetos únicos: {}", hdt.getDictionary().getNsubjects());
                LOG.info("Número de predicados únicos: {}", hdt.getDictionary().getNpredicates());
                LOG.info("Número de objetos únicos: {}", hdt.getDictionary().getNobjects());
            } else {
                LOG.error("La generación del HDT resultó en un objeto nulo.");
            }

        } catch (Exception e) {
            // Log para informar cualquier excepción
            LOG.error("Error al generar HDT desde el archivo: {}", filePath, e);
        }

        return hdt;
    }
}
