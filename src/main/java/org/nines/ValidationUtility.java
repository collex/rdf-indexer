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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

//import org.jdom.Element;
//import org.jdom.IllegalDataException;
//import org.jdom.Verifier;

public class ValidationUtility {

    // List of all valid genres
	public static final String[] GENRE_LIST = new String[] {
		 "Advertisement", "Animation", "Bibliography", "Catalog", "Chronology", "Citation", "Collection",
		 "Correspondence", "Criticism", "Drama", "Ephemera", "Essay", "Fiction", "Film, Documentary", 
		 "Film, Experimental", "Film, Narrative", "Film, Other", "Historiography", "Interview", "Life Writing",
		 "Liturgy", "Musical Analysis", "Music, Other", "Musical Work", "Musical Score", "Nonfiction", "Paratext",
		 "Performance", "Philosophy", "Photograph", "Political Statement", "Poetry", "Religion", "Reference Works",
		 "Review", "Scripture", "Sermon", "Speech", "Translation", "Travel Writing", "Unspecified", "Visual Art"	
	};

    // List of all valid disciplines
    public static final String[] DISCIPLINE_LIST = new String [] {
    	"Anthropology", "Archaeology", "Architecture", "Art History", "Art Studies", "Book History", "Classics and Ancient History", 
    	"Dance Studies", "Economics", "Education", "Ethnic Studies", "Film Studies", "Gender Studies", "Geography", 
    	"History", "Labor Studies", "Law", "Literature", "Manuscript Studies", "Math", "Music Studies", "Philosophy", 
    	"Political Science", "Religious Studies", "Science", "Sociology", "Sound Studies", "Theater Studies"
    };

    // List of all valid types
    public static final String[] TYPE_LIST = new String [] {
    	"Codex", "Collection", "Dataset", "Drawing", "Illustration", "Interactive Resource", "Manuscript", "Map", "Moving Image", 
    	"Notated Music", "Page Proofs", "Pamphlet", "Periodical", "Physical Object", "Roll", "Sheet", "Sound", "Still Image", "Typescript"	
    };

    // Fields that are required to be present in RDF
    public static final String[] REQUIRED_FIELDS = new String[] { "archive", "title", "year", "doc_type", "genre",  "discipline",
        "freeculture", "has_full_text", "is_ocr", "federation", "url"  };

    // Parallel to required fields - the actual tag name to be used
    // Any change above must be reflected here
    private static final String[] RDF_TERM = new String[] { "collex:archive", "dc:title", "dc:date", "dc:type", "collex:genre", "collex:discipline",
        "collex:freeculture", "collex:full_text", "collex:is_ocr", "collex:federation", "rdfs:seeAlso"};
    
    public static final String[] REQUIRED_PAGE_FIELDS = new String[] { "text", "page_of", "page_num" };
    private static final String[] RDF_PAGE_TERM = new String[] { "collex:text", "collex:pageof", "collex:pagenum"};

    private static final String[] ROLE_FIELDS = new String[] {
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
    	"role_WAM", "role_WAT", "role_WDC", "role_WDE", "role_WIN", "role_WIT", "role_WPR", "role_WST"	
    };

    public static ArrayList<String> validateObject(boolean isPagesArchive, HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        if ( !isPagesArchive ) {
            messages.addAll(ValidationUtility.validateRequired(object));
            messages.addAll(ValidationUtility.validateGenre(object));
            messages.addAll(ValidationUtility.validateDiscipline(object));
            messages.addAll(ValidationUtility.validateRole(object));
            messages.addAll(ValidationUtility.validateType(object));
            messages.addAll(ValidationUtility.validateUri(object));
        } else {
            messages.addAll(ValidationUtility.validatePagesRequired(object));
            messages.addAll(ValidationUtility.validateUri(object));  
        }

        return messages;
    }

    public static ArrayList<String> validateRole(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        // look for all role_* keys in object, validate that they are in this list:
        //    ART, AUT, EDT, PBL, and TRL, also CRE, EGR, ETR
        ArrayList<String> validRoles = new ArrayList<String>(Arrays.asList(ROLE_FIELDS));

        Set<String> keys = object.keySet();
        for (String key : keys) {
            if (key.startsWith("role_")
                &&  !validRoles.contains(key) ) {
                messages.add("invalid role: " + key);
            }
        }

        return messages;
    }

    public static ArrayList<String> validateUri(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        // The URI can't contain foo
        ArrayList<String> fields = object.get("uri");
        if (fields.size() > 1)
            messages.add("must contain exactly one URI field");
        if (!fields.isEmpty()) {
            String fieldVal = fields.get(0);
            if (fieldVal.startsWith("http://foo/"))
                messages.add("URI field is not created properly");
        }

        return messages;
    }

	private static void maxOne(String fieldName, HashMap<String, ArrayList<String>> object, ArrayList<String> messages) {
        ArrayList<String> fields = object.get(fieldName);
        if (fields != null && fields.size() > 1) {
            String f = "";
            for (String s : fields) {
                f += s + ";";
            }
            messages.add("must not contain more than one " + fieldName + " field:" + f);
            while (fields.size() > 1)
            	fields.remove(1);
        }
	}
	
	/**
     * Confirms that required fields for PAGES archives are present and non-null
     */
    public static ArrayList<String> validatePagesRequired(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        for (int i = 0; i < REQUIRED_PAGE_FIELDS.length; i++) {
            if (!object.containsKey(REQUIRED_PAGE_FIELDS[i])) {
                messages.add("object must contain the " + RDF_PAGE_TERM[i] + " field");
            }
        }
        
        if ( object.containsKey("text") ) {
            ArrayList<String> txtVal = object.get("text");
            if ( txtVal.get(0).length() == 0) {
                object.remove("text");
                messages.add("Warning - collex:text is blank");
            }
        }
        return messages;
    }
	
    /**
     * Confirms that required fields are present and non-null
     */
    public static ArrayList<String> validateRequired(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        for (int i = 0; i < REQUIRED_FIELDS.length; i++) {
            if (!object.containsKey(REQUIRED_FIELDS[i])) {
                messages.add("object must contain the " + RDF_TERM[i] + " field");
            }
        }

        ArrayList<String> fields = object.get("archive");
        if (fields == null || fields.size() > 1) {
            messages.add("must contain exactly one archive field");
        }

		maxOne("title", object, messages);
		maxOne("url", object, messages);
		maxOne("thumbnail", object, messages);

        Set<String> keys = object.keySet();
        boolean hasRole = false;
        for (String key : keys) {
            if (key.startsWith("role_")) {
                hasRole = true;
                break;
            }
        }

        if (!hasRole) {
            messages.add("object must contain at least one role:XXX field");
        }

        return messages;
    }

    /**
     * The genre must be in a constrained list.
     */
    public static ArrayList<String> validateGenre(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        for (Map.Entry<String, ArrayList<String>> entry : object.entrySet()) {

            String key = entry.getKey();
            ArrayList<String> valueList = entry.getValue();

            if ("genre".equals(key)) {
                // test 1: each genre is valid
                for (String genre : valueList) {
                    if (!validateGenreInList(genre)) {
                        messages.add(genre + " genre not approved by ARC");
                    }
                }
            }
        }

        return messages;
    }

    public static boolean validateGenreInList(String genre) {

        for (String aGenreList : GENRE_LIST) {
            if (aGenreList.equals(genre)) {
                return true;
            }
        }

        return false;
    }

    /**
     * The genre must be in a constrained list.
     */
    public static ArrayList<String> validateDiscipline(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        for (Map.Entry<String, ArrayList<String>> entry : object.entrySet()) {

            String key = entry.getKey();
            ArrayList<String> valueList = entry.getValue();

            if ("discipline".equals(key)) {
                // test 1: each discipline is valid
                for (String discipline : valueList) {
                    if (!validateDisciplineInList(discipline)) {
                        messages.add(discipline + " discipline not approved by ARC");
                    }
                }
            }
        }

        return messages;
    }



    public static boolean validateDisciplineInList(String discipline) {
        for (String aDisciplineList : DISCIPLINE_LIST) {
            if (aDisciplineList.equals(discipline)) {
                return true;
            }
        }

        return false;
    }

    /**
     * The genre must be in a constrained list.
     */
    public static ArrayList<String> validateType(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();
        for (Map.Entry<String, ArrayList<String>> entry : object.entrySet()) {

            String key = entry.getKey();
            ArrayList<String> valueList = entry.getValue();

            if ("doc_type".equals(key)) {
                // test 1: each type is valid
                for (String type : valueList) {
                    if (!validateTypeInList(type)) {
                        messages.add(type + " type not approved by ARC");
                    }
                }
            }
        }
        return messages;
    }

    public static boolean validateTypeInList(String type) {
        for (String aTypeList : TYPE_LIST) {
            if (aTypeList.equals(type)) {
                return true;
            }
        }

        return false;
    }

    public static ArrayList<String> validateFreecultureElement(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        ArrayList<String> fields = object.get("freeculture");
        String fieldVal = fields.get(0);
        if (!validateFreeculture(fieldVal)) {
            messages.add(fieldVal + " is not a valid value for collex:freeculture");
        }

        return messages;
    }

    public static boolean validateFreeculture(String value) {
        if (value == null || "true".equals(value) || "false".equals(value))
            return true;
        else
            return false;
    }
}


