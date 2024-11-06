package runnertest;

import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDTTest {
    private static final Logger LOG = LoggerFactory.getLogger(HDTTest.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Por favor, proporciona la ruta al archivo .nt como argumento.");
            return;
        }

        String filePath = args[0];
        HDT hdt = null;

        try {
            LOG.info("Intentando generar HDT desde el archivo: {}", filePath);
            hdt = HDTManager.generateHDT(filePath, "http://example.org/baseURI", RDFNotation.NTRIPLES, new HDTSpecification(), null);
            LOG.info("Generación de HDT exitosa.");

        } catch (Exception e) {
            LOG.error("Error al generar HDT desde el archivo: {}", filePath, e);
        } finally {
            if (hdt != null) {
                try {
                    hdt.close();
                } catch (Exception e) {
                    LOG.error("Error al cerrar HDT", e);
                }
            }
        }
    }
}
