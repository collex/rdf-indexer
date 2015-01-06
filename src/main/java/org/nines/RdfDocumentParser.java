/** 
 *  Copyright 2011 Applied Research in Patacriticism and the University of Virginia
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 **/
package org.nines;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.openrdf.rio.ParseErrorListener;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.rdfxml.RDFXMLParser;

public class RdfDocumentParser {
    private static long largestTextSize = 0;
    public final static Logger log = Logger.getLogger(RdfDocumentParser.class.getName());

    public static long getLargestTextSize() {
        return largestTextSize;
    }

    public static HashMap<String, HashMap<String, ArrayList<String>>> parse(final File file, ErrorReport errorReport,
            LinkCollector linkCollector, RDFIndexerConfig config) throws IOException {

        largestTextSize = 0;
        RDFXMLParser parser = new RDFXMLParser();
        NinesStatementHandler statementHandler = new NinesStatementHandler(errorReport, linkCollector, config);
        statementHandler.setFile(file);

        parser.setRDFHandler(statementHandler);
        parser.setParseErrorListener( new ParseListener(file, errorReport));
        parser.setVerifyData(true);
        parser.setStopAtFirstError(false);

        // parse file
        try {
            
            String content = validateContent(file, errorReport);
            parser.parse( new StringReader(content), "http://foo/" + file.getName());

        } catch (RDFParseException e) {
            errorReport.addError(new IndexerError(file.getName(), "", "Parse Error on Line " + e.getLineNumber() + ": "
                    + e.getMessage()));
        } catch (RDFHandlerException e) {
            errorReport.addError(new IndexerError(file.getName(), "", "StatementHandler Exception: " + e.getMessage()));
        } catch (Exception e) {
            errorReport.addError(new IndexerError(file.getName(), "", "RDF Parser Error: " + e.getMessage()));
            e.printStackTrace();
        }

        // retrieve parsed data
        HashMap<String, HashMap<String, ArrayList<String>>> docHash = statementHandler.getDocuments( config.isPagesArchive() );

        // process tags
        Collection<HashMap<String, ArrayList<String>>> documents = docHash.values();
        for (HashMap<String, ArrayList<String>> document : documents) {

            // normalize tags, replace spaces with dashes, lowercase
            ArrayList<String> tags = document.remove("tag");
            if (tags != null) {
                for (int i = 0; i < tags.size(); i++) {
                    String tag = tags.get(i);
                    tag = tag.toLowerCase();
                    tag = tag.replaceAll(" ", "-");
                    tags.set(i, tag);
                }
                // username is archive name
                String archive = document.get("archive").get(0);
                ArrayList<String> nameList = new ArrayList<String>();
                nameList.add(archive);
                document.put("username", nameList);
                document.put(archive + "_tag", tags);
            }
        }

        largestTextSize = statementHandler.getLargestTextSize();
        return docHash;
    }

    private static String validateContent(File file, ErrorReport errorReport) {
        InputStreamReader is = null;
        try {
            Charset cs = Charset.availableCharsets().get("UTF-8");
            CharsetDecoder decoder = cs.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
            
            is = new InputStreamReader(new FileInputStream(file), decoder);
            String content = IOUtils.toString(is);

            // look for unescaped sequences and flag them as trouble
            String unescaped = StringEscapeUtils.unescapeXml(content);
            int startPos = 0;
            while ( true ) {
              int pos = unescaped.indexOf("&#", startPos);
              if (pos > -1) {
                String snip = unescaped.substring(Math.max(0, pos-25), Math.min(unescaped.length(), pos+25));
                IndexerError e = new IndexerError(file.getName(), "","Potentially Invalid Escape sequence.\n   Position: [" +
                    pos + "]\n   Snippet: [" +
                    snip + "]");
                errorReport.addError(e);
                startPos = pos+2;
              } else {
                break;
              }
            }
            
        
            return content;
        } catch (IOException e) {
            errorReport.addError(new IndexerError(file.getName(), "", "Error validating content: " + e.getMessage()));
        } finally {
            IOUtils.closeQuietly(is);
        }
        return "";
    }
    
    private static final class ParseListener implements ParseErrorListener {

        private ErrorReport errorReport;
        private File file;
        
        ParseListener(File file, ErrorReport errorReport ) {
            this.errorReport   = errorReport;
            this.file = file;
        }
        public void warning(String msg, int lineNo, int colNo) {
            this.errorReport.addError(new IndexerError(file.getName(), "", 
                "Parse warning at line "+lineNo+", col "+colNo+" : " + msg));   
        }

        public void error(String msg, int lineNo, int colNo) {
            this.errorReport.addError(new IndexerError(file.getName(), "", 
                "Parse error at line "+lineNo+", col "+colNo+" : " + msg)); 
        }

        public void fatalError(String msg, int lineNo, int colNo) {
            this.errorReport.addError(new IndexerError(file.getName(), "", 
                "FATAL PARSE ERROR at line "+lineNo+", col "+colNo+" : " + msg)); 
        }
        
    }
}
