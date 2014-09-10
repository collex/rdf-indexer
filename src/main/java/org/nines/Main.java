package org.nines;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

public class Main {

    /**
     * MAIN Main application entry point
     *
     * @param args
     */
    public static void main(String[] args) {

        // Option constants
        final String logDir = "logDir";         // logging directory
        final String deleteFlag = "delete";     // delete an archive from solr
        final String mode = "mode";             // REQUIRED mode of operation: [TEST, SPIDER, CLEAN, INDEX, COMPARE]
        final String ignoreFlag = "ignore";     // A list of fields to ignore
        final String includeFlag = "include";   // A list of fields to include
        final String source = "source";         // index: REQUIRED path to archive
        final String archive = "archive";       // REQUIRED name of archive
        final String pageSize = "pageSize";     // compare: max results per solr page
        final String maxSize = "maxSize";       // indexing: the max size of data to send to solr
        final String custom = "custom";         // flag to indicate customized clean
        final String encoding = "encoding";     // set char set of raw source text for clea

        // define the list of command line options
        Options options = new Options();
        options.addOption( source, true, "Path to the target RDF archive directory" );
        options.addOption( archive, true, "The name of of the archive");
        options.getOption( archive).setRequired(true);
        options.addOption( mode, true, "Mode of operation [TEST, SPIDER, CLEAN_RAW, CLEAN_FULL, INDEX, RESOLVE, COMPARE]" );
        options.getOption( mode).setRequired(true);

        // include/exclude field group
        OptionGroup fieldOpts = new OptionGroup();
        fieldOpts.addOption(new Option(ignoreFlag, true,
                "Comma separated list of fields to ignore in compare. Default is none."));
        fieldOpts.addOption(new Option(includeFlag, true,
                "Comma separated list of fields to include in compare. Default is all."));
        options.addOptionGroup(fieldOpts);

        options.addOption(deleteFlag, false, "Delete ALL items from an existing archive");
        options.addOption(logDir, true, "Set the root directory for all indexer logs");
        options.addOption(pageSize, true,
                "Set max documents returned per solr page. Default = 500 for most, 1 for special cases");

        options.addOption(encoding, true, "Encoding of source raw text file for clean");
        options.addOption(custom, true, "Customized clean class");

        // create parser and handle the options
        RDFIndexerConfig config = new RDFIndexerConfig();
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine line = parser.parse(options, args);

            // required params:
            config.archiveName = line.getOptionValue(archive);
            if (line.hasOption(mode)) {
                String modeVal = line.getOptionValue(mode).toUpperCase();
                config.mode = RDFIndexerConfig.Mode.valueOf( modeVal );
                if (config.mode == null) {
                    throw new ParseException("Invalid mode " + modeVal);
                }
            }

            // optional params:
            if (line.hasOption(source)) {
                config.sourceDir = new File(line.getOptionValue(source));
            }
            if (line.hasOption(maxSize)) {
                config.maxUploadSize = Long.parseLong(line.getOptionValue(source));
            }
            if (line.hasOption(pageSize)) {
                config.pageSize = Integer.parseInt(line.getOptionValue(pageSize));
            }
            if (line.hasOption(logDir)) {
                config.logRoot = line.getOptionValue(logDir);
            }
            config.deleteAll = line.hasOption(deleteFlag);

            // compare stuff
            if (line.hasOption(includeFlag)) {
                config.includeFields = line.getOptionValue(includeFlag);
            }
            if (line.hasOption(ignoreFlag)) {
                config.ignoreFields = line.getOptionValue(ignoreFlag);
            }

            // if we are indexing, make sure source is present
            switch( config.mode ) {
                case CLEAN_RAW:
                case CLEAN_FULL:
                case INDEX:
                    if( config.sourceDir == null ) {
                        throw new ParseException("Missing required -source parameter");
                    }
                default:
                    break;
            }

            if (line.hasOption(encoding)) {
                config.defaultEncoding = line.getOptionValue(encoding);
            }

            if (line.hasOption(custom)) {
                config.customCleanClass = line.getOptionValue(custom);
            }

        } catch (ParseException exp) {

            System.out.println("Error parsing options: " + exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("rdf-idexer", options);
            System.exit(-1);
        }

//        // Use the SAX2-compliant Xerces parser:
//        System.setProperty("org.xml.sax.driver", "org.apache.xerces.parsers.SAXParser");

        // Launch the task
        try {
            if (config.mode.equals( RDFIndexerConfig.Mode.COMPARE)) {
                RDFCompare task = new RDFCompare(config);
                task.compareArchive();
            } else {
                RDFIndexer task = new RDFIndexer(config);
                task.execute();
            }
        } catch (Exception e) {
            Logger.getLogger( RDFIndexer.class.getName() ).error("Unhandled exception: " + e.toString());
            StringWriter result = new StringWriter();
            PrintWriter printWriter = new PrintWriter(result);
            e.printStackTrace(printWriter);
            Logger.getLogger(RDFIndexer.class.getName()).error(result.toString());
        }
        System.exit(0);
    }
}
