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

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.*;

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
        RESOLVE,    // examine existing archive and resolve any references (isPartOf, hasPart)
        COMPARE     // compare the new arcive with the main index
    };

    // general properties
    public String logRoot = ".";
    public File sourceDir;
    public String archiveName;
    public String solrBaseURL = "http://localhost:8983/solr";
    public Mode mode = Mode.NONE;
    public String defaultEncoding = "UTF-8";
    public String customCleanClass = "";
    
    // corrected text map: URI -> filename
    public Map<String,String> correctedTextMap =  new HashMap<String,String>();
    public File correctedTextDir = null;

    // indexing properties
    public boolean collectLinks = true;
    public boolean deleteAll = false;
    public long maxUploadSize = 10000000; // 10m of characters

    // comparison properties
    public String ignoreFields = "";
    public String includeFields = "*";
    public int pageSize = 500;
    
    public boolean isPagesArchive() {
        return (this.archiveName.indexOf("pages_") == 0);
    }

    public final boolean isTestMode() {
        return this.mode.equals(Mode.TEST);
    }

    // all of the solr instance fields. Text is the last field
    private static final ArrayList<String> ALL_FIELDS = new ArrayList<String>( Arrays.asList( "uri", "archive",
            "date_label", "genre", "source", "image", "thumbnail", "title", "alternative", "url", "role_ART", "role_AUT",
            "role_BRD", "role_CNG", "role_CND", "role_DRT", "role_IVR", "role_IVE", "role_OWN", "role_FMO", "role_PRF", "role_PRO", "role_PRN",
            "role_EDT", "role_PBL", "role_TRL", "role_EGR", "role_ETR", "role_CRE", "freeculture", "is_ocr", "federation",
            "has_full_text", "source_xml", "typewright", "publisher", "agent", "agent_facet", "author", "editor",
            "text_url", "year", "type", "date_created", "date_updated", "title_sort", "author_sort",
            "year_sort", "source_html",
            "hasPart", "isPartOf",
            "source_sgml", "person", "format", "language", "geospacial", "text" ));
    
    private static final ArrayList<String> ALL_PAGE_FIELDS = new ArrayList<String>( Arrays.asList( "uri", "archive",
        "date_created", "date_updated", "page_num", "page_of", "text" ));


    /**
     * Gets the path and partial name of the logfile. The partial name
     * just includes the cleaned arhive name. To this must be appended
     * the log type and extension (ex: _progress.log)
     * 
     * @return Full path and base name of logfile
     */
    public final String getLogfileBaseName(String subFolder) {
        String name = this.archiveName.replaceAll("/", "_").replaceAll(":", "_").replaceAll(" ", "_");
        String logFileRelativePath = this.logRoot + "/";
		if (!subFolder.equals(""))
			logFileRelativePath = logFileRelativePath + subFolder + "/";
        return logFileRelativePath + name;
    }

    /**
     * Look at the compare config and generate a field list
     * suitable for submission to Solr:
     * @return List in the form: field1+field2+...
     */
    public final String getFieldList() {

        ArrayList<String> fields = ALL_FIELDS;
        if ( isPagesArchive() ) {
            fields = ALL_PAGE_FIELDS;
        }
        
        // if the ignored list has anything assume all fields and skip requested
        if (ignoreFields.trim().length() > 0) {
            List<String> ignored = new ArrayList<String>(Arrays.asList(ignoreFields.split(",")));
            List<String> fl = new ArrayList<String>( fields );
            for (String ignore : ignored) {
                fl.remove(ignore);
            }
            return StringUtils.join( fl.iterator(), "+" );
        }

        // all fields?
        if (includeFields.equals("*")) {
            return "*";
        }

        // just some
        List<String> included = new ArrayList<String>(Arrays.asList(includeFields.split(",")));
        if (included.contains("uri") == false) {
            included.add("uri");
        }

        return StringUtils.join(included.iterator(), "+");
    }

    /**
     * Generate a clean core name from an archive
     */
    public final String coreName( ) {
        return( coreName( archiveName ) );
    }

    public final String coreName( final String archive ) {
        if (archive.indexOf("pages_") == 0) {
            return  safeArchive( archive );
        }
        return "archive_" + safeArchive( archive );
    }

    public static final String safeArchive( String archive ) {
        archive = archive.replaceAll(":", "_");
        archive = archive.replaceAll(" ", "_");
        archive = archive.replaceAll(",", "_");
        return( archive );
    }

}
