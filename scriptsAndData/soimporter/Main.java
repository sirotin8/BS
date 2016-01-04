package org.stackexchange.dumps.importer;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.stackexchange.dumps.importer.contexts.ImporterContext;
import org.stackexchange.dumps.importer.contexts.PostgresImporterContext;
import org.stackexchange.dumps.importer.services.GenericImporter;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;

/**
 * This class can be used to start the importer from the command line, without any Java/Spring knowledge.
 */
public class Main {

	public static String con;

    public static void main(String [] args) throws FileNotFoundException, JAXBException, ClassNotFoundException {
        if (args.length == 2) {
		con = args[0];	
            importDirectory(args[1]);
            return;
        }
        if (args.length > 3)
            throw new RuntimeException("Exactly three arguments expected!");
		con = args[0];	
        importFile(args);
    }

    private static void importFile(String [] args) throws ClassNotFoundException, FileNotFoundException, JAXBException {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ImporterContext.class);
        GenericImporter importer = context.getBean(GenericImporter.class);
        Class clazz = Class.forName("org.stackexchange.dumps.importer.domain." + args[1]);
        importer.importFile(Long.MAX_VALUE, args[2], clazz);
    }

    private static void importDirectory(String directory) throws FileNotFoundException, JAXBException {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ImporterContext.class);
        GenericImporter importer = context.getBean(GenericImporter.class);
        importer.importDirectory(directory);
    }
}
