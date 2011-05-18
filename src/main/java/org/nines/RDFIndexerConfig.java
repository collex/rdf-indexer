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

    // mode of operation
    public enum Mode {
        NONE,       // No mode.. do nothing. 
        TEST,       // Testing mode: ignore all ext text, dont post to solr
        SPIDER,     // retrieve full text from external source - no post to solr
        CLEAN_RAW,  // cleanup the raw sipdered text and move to fulltext
        CLEAN_FULL, // cleanup the fulltext 
        INDEX,      // populate solr with rdf data Text will be pulled from the RDF or fulltext
        COMPARE     // compare the new arcive with the main index
    };

    // general properties
    public String logRoot = ".";
    public File sourceDir;
    public String archiveName;
    public String solrBaseURL = "http://localhost:8983/solr";
    public String solrExistingIndex = "/resources";
    public Mode mode = Mode.NONE;
    public String encoding = "UTF-8";
    
    public String customCleanClass = "";

    // indexing properties
    public boolean collectLinks = true;
    public boolean deleteAll = false;
    public long maxUploadSize = 10000000; // 10m of characters

    // comparison properties
    public String ignoreFields = "";
    public String includeFields = "*";
    public int pageSize = 500;

    public final boolean isTestMode() {
        return this.mode.equals(Mode.TEST);
    }
    
    /**
     * Gets the path and partial name of the logfile. The partial name
     * just includes the cleaned arhive name. To this must be appended
     * the log type and extension (ex: _progress.log)
     * 
     * @return Full path and base name of logfile
     */
    public final String getLogfileBaseName() {
        String name = this.archiveName.replaceAll("/", "_").replaceAll(":", "_").replaceAll(" ", "_");
        String logFileRelativePath = this.logRoot + "/";
        return logFileRelativePath + name;
    }
}
