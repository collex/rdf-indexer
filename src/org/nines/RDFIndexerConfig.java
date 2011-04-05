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

/**
 * Configuration for the RDFFileIndexer
 * 
 * @author nicklaiacona
 */
public class RDFIndexerConfig {
  
  //Modes for handling docs with ecternal text
  public enum TextMode {
    DEBUG,          // default mode with no params. ignore all ext text
    RETRIEVE_FULL,  // retrieve full text from web
    REINDEX_FULL    // pull full text from existing index
  };

  public String logRoot = ".";
  public File rdfSource;
  public String archiveName;
  public String solrBaseURL = "http://localhost:8983/solr";
  public String solrExistingIndex = "/resources";
  public boolean collectLinks = true;
  public TextMode textMode = TextMode.DEBUG;
  public boolean compare = false;
  public int maxDocsPerFolder = 99999999;
  public String ignoreFields = "";
  public String includeFields = "*";
  public boolean commitToSolr = true;
  public boolean deleteAll = false;
  public int pageSize = 500;
  public int numThreads = 5;
}
