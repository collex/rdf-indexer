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
            "date_label", "genre", "source", "image", "thumbnail", "title", "alternative", "url", 
            "role_ABR", "role_ACP", "role_ACT", "role_ADI", "role_ADP", "role_AFT", "role_ANL", "role_ANM", "role_ANN", "role_ANT",
        	"role_APE", "role_APL", "role_APP", "role_AQT", "role_ARC", "role_ARD", "role_ARR", "role_ART", "role_ASG", "role_ASN",
        	"role_ATO", "role_ATT", "role_AUC", "role_AUD", "role_AUI", "role_AUS", "role_AUT", "role_BDD", "role_BJD", "role_BKD",
        	"role_BKP", "role_BLW", "role_BND", "role_BPD", "role_BRD", "role_BRL", "role_BSL", "role_CAS", "role_CCP", "role_CHR",
        	"role_CLI", "role_CLL", "role_CLR", "role_CLT", "role_CMM", "role_CMP", "role_CMT", "role_CND", "role_CNG", "role_CNS",
        	"role_COE", "role_COL", "role_COM", "role_CON", "role_COR", "role_COS", "role_COT", "role_COU", "role_COV", "role_CPC",
        	"role_CPE", "role_CPH", "role_CPL", "role_CPT", "role_CRE", "role_CRP", "role_CRR", "role_CRT", "role_CSL", "role_CSP",
        	"role_CST", "role_CTB", "role_CTE", "role_CTG", "role_CTR", "role_CTS", "role_CTT", "role_CUR", "role_CWT", "role_DBP",
        	"role_DFD", "role_DFE", "role_DFT", "role_DGG", "role_DGS", "role_DIS", "role_DLN", "role_DNC", "role_DNR", "role_DPC",
        	"role_DPT", "role_DRM", "role_DRT", "role_DSR", "role_DST", "role_DTC", "role_DTE", "role_DTM", "role_DTO", "role_DUB",
        	"role_EDC", "role_EDM", "role_EDT", "role_EGR", "role_ELG", "role_ELT", "role_ENG", "role_ENJ", "role_ETR", "role_EVP",
        	"role_EXP", "role_FAC", "role_FDS", "role_FLD", "role_FLM", "role_FMD", "role_FMK", "role_FMO", "role_FMP", "role_FND",
        	"role_FPY", "role_FRG", "role_GIS", "role_HIS", "role_HNR", "role_HST", "role_ILL", "role_ILU", "role_INS", "role_INV",
        	"role_ISB", "role_ITR", "role_IVE", "role_IVR", "role_JUD", "role_JUG", "role_LBR", "role_LBT", "role_LDR", "role_LED",
        	"role_LEE", "role_LEL", "role_LEN", "role_LET", "role_LGD", "role_LIE", "role_LIL", "role_LIT", "role_LSA", "role_LSE",
        	"role_LSO", "role_LTG", "role_LYR", "role_MCP", "role_MDC", "role_MED", "role_MFP", "role_MFR", "role_MOD", "role_MON",
        	"role_MRB", "role_MRK", "role_MSD", "role_MTE", "role_MTK", "role_MUS", "role_NRT", "role_OPN", "role_ORG", "role_ORM",
        	"role_OSP", "role_OTH", "role_OWN", "role_PAN", "role_PAT", "role_PBD", "role_PBL", "role_PDR", "role_PFR", "role_PHT",
        	"role_PLT", "role_PMA", "role_PMN", "role_POP", "role_PPM", "role_PPT", "role_PRA", "role_PRC", "role_PRD", "role_PRE",
        	"role_PRF", "role_PRG", "role_PRM", "role_PRN", "role_PRO", "role_PRP", "role_PRS", "role_PRT", "role_PRV", "role_PTA",
        	"role_PTE", "role_PTF", "role_PTH", "role_PTT", "role_PUP", "role_RBR", "role_RCD", "role_RCE", "role_RCP", "role_RDD",
        	"role_RED", "role_REN", "role_RES", "role_REV", "role_RPC", "role_RPS", "role_RPT", "role_RPY", "role_RSE", "role_RSG",
        	"role_RSP", "role_RSR", "role_RST", "role_RTH", "role_RTM", "role_SAD", "role_SCE", "role_SCL", "role_SCR", "role_SDS",
        	"role_SEC", "role_SGD", "role_SGN", "role_SHT", "role_SLL", "role_SNG", "role_SPK", "role_SPN", "role_SPY", "role_SRV",
        	"role_STD", "role_STG", "role_STL", "role_STM", "role_STN", "role_STR", "role_TCD", "role_TCH", "role_THS", "role_TLD",
        	"role_TLP", "role_TRC", "role_TRL", "role_TYD", "role_TYG", "role_UVP", "role_VAC", "role_VDG", "role_WAC", "role_WAL",
        	"role_WAM", "role_WAT", "role_WDC", "role_WDE", "role_WIN", "role_WIT", "role_WPR", "role_WST",
        	"freeculture", "is_ocr", "federation",
            "has_full_text", "source_xml", "typewright", "publisher", "agent", "agent_facet", "author", "editor",
            "text_url", "year", "type", "date_created", "date_updated", "title_sort", "author_sort",
            "year_sort", "source_html",
            "hasPart", "isPartOf",
            "source_sgml", "person", "format", "language", "geospacial", "text", "coverage", "description", "review_date"));
    
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
