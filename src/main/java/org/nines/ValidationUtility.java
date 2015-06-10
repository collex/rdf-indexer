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
        "Bibliography",  "Catalog",  "Citation",  "Collection",  "Correspondence",  "Criticism",  "Drama",  "Ephemera",
        "Fiction",  "Historiography",  "Law",  "Life Writing", "Liturgy",  "Music, Other",  "Musical Analysis",
        "Musical Recording",  "Musical Score",  "Nonfiction",  "Paratext",  "Philosophy",  "Photograph",  "Poetry",
        "Religion", "Religion, Other",  "Reference Works",  "Review", "Scripture",
        "Sermon",  "Translation",  "Travel Writing",  "Unspecified",  "Visual Art"
    };

    // List of all valid disciplines
    public static final String[] DISCIPLINE_LIST = new String [] {
        "Anthropology", "Archaeology", "Architecture", "Art History", "Book History", "Classics and Ancient History",
        "Film Studies", "Theater Studies", "Ethnic Studies", "Gender Studies", "Geography", "Philosophy", "History",
        "Science", "Law", "Literature", "Musicology", "Math", "Religious Studies", "Manuscript Studies"
    };

    // List of all valid types
    public static final String[] TYPE_LIST = new String [] {
        "Codex",  "Collection",  "Drawing",  "Illustration", "Interactive Resource",  "Manuscript",  "Map",  "Moving Image",  "Periodical",
        "Physical Object", "Roll", "Sheet",  "Sound",  "Still Image",  "Typescript"
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

    private static final String[] ROLE_FIELDS = new String[] { "role_ART", "role_AUT", "role_EDT", "role_PBL", "role_CRE",
		"role_BRD","role_CNG","role_CND","role_DRT","role_IVR","role_IVE","role_OWN","role_FMO","role_PRF","role_PRO","role_PRN",
       "role_EGR", "role_ETR", "role_TRL", "role_ARC", "role_BND", "role_BKD", "role_BKP", "role_CLL", "role_CTG", "role_COL",
        "role_CLR", "role_CWT", "role_COM", "role_CMT", "role_CRE", "role_DUB", "role_FAC", "role_ILU", "role_ILL", "role_LTG",
        "role_PRT", "role_POP", "role_PRM", "role_RPS", "role_RBR", "role_SCR", "role_SCL", "role_TYD", "role_TYG", "role_WDE",
        "role_WDC", "role_OWN" };

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


