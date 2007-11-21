/** 
 *  Copyright 2007 Applied Research in Patacriticism and the University of Virginia
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

import org.apache.commons.httpclient.HttpClient;
import org.openrdf.sesame.sailimpl.memory.RdfSource;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.rio.Parser;
import org.openrdf.rio.ParseErrorListener;
import org.openrdf.rio.StatementHandlerException;
import org.openrdf.rio.ParseException;
import org.openrdf.rio.rdfxml.RdfXmlParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.File;
import java.util.*;
import org.apache.log4j.Logger;

public class RdfDocumentParser {
  public final static Logger log = Logger.getLogger(RdfDocumentParser.class.getName());

  public static HashMap<String, HashMap<String, ArrayList<String>>> parse(final File file, ErrorReport errorReport, LinkCollector linkCollector, RDFIndexerConfig config ) throws IOException {
    RdfSource rdfSource = new RdfSource();
    try {
      rdfSource.initialize();
    } catch (SailInitializationException e) {
      throw new IOException(e.getMessage());
    }
    Parser parser = new RdfXmlParser(rdfSource);
    NinesStatementHandler statementHandler = new NinesStatementHandler(errorReport, linkCollector, config);
    statementHandler.setFilename(file.getName());

    parser.setStatementHandler(statementHandler);
    parser.setParseErrorListener(new ParseErrorListener() {
      public void warning(String string, int i, int i1) {
        log.info("warning = " + string);
      }

      public void error(String string, int i, int i1) {
        log.info("error = " + string);
      }

      public void fatalError(String string, int i, int i1) {
        log.info("fatalError = " + string);
      }
    });
    parser.setVerifyData(true);
    parser.setStopAtFirstError(true);

    // parse file
    try {
      parser.parse(new InputStreamReader(new FileInputStream(file), "UTF8"), "http://foo/bar");
    } catch (ParseException e) {   
      errorReport.addError(new Error(file.getName(), "", "Parse Error on Line "+e.getLineNumber()+": "+e.getMessage() ));
    } catch (StatementHandlerException e) {
      errorReport.addError(new Error(file.getName(), "", "StatementHandler Exception: "+e.getMessage()));
    } catch( Exception e ) {
  	  errorReport.addError(new Error(file.getName(), "", "RDF Parser Error: "+e.getMessage()));
	}
    
    // retrieve parsed data 
    HashMap<String, HashMap<String, ArrayList<String>>> docHash = statementHandler.getDocuments();

    // process tags
    Collection<HashMap<String, ArrayList<String>>> documents = docHash.values();
    for (HashMap<String, ArrayList<String>> document : documents) {

      // normalize tags, replace spaces with dashes, lowercase
      ArrayList<String> tags = document.remove("tag");
      if (tags != null) {
        for (int i=0; i < tags.size(); i++) {
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

    return docHash;
  }
}
