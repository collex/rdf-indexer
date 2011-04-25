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
  
  // indexing mode
  public enum IndexMode {
    SKIP,       // Do not index
    TEST,       // Testing mode: ignore all ext text, dont post to solr
    FULL,       // retrieve full text from web
    REINDEX     // pull full text from existing index
  };

  // general properties
  public String logRoot = ".";
  public File rdfSource;
  public String archiveName;
  public String solrLegacyURL = "http://localhost:8983/solr";
  public String solrBaseURL = "http://localhost:8984/solr";
  public String solrExistingIndex = "/resources";
  
  // indexing properties
  public boolean collectLinks = true;
  public boolean deleteAll = false;
  public IndexMode indexMode = IndexMode.SKIP;
  public boolean upgadeSolr=false;      // used when reindex from solr 1.4 -> 3.1
                                        // pulls external text from 1.4 version
  public long maxUploadSize = 10000000; // 10m of characters
  
  // comparison properties
  public boolean compare = false;
  public String ignoreFields = "";
  public String includeFields = "*";
  public int pageSize = 500;
  
  public final boolean isTestMode() {
      return this.indexMode.equals(IndexMode.TEST);
  }
}
