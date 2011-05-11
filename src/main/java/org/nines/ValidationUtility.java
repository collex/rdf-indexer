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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jdom.Element;
import org.jdom.IllegalDataException;
import org.jdom.Verifier;

public class ValidationUtility {

    // List of all valid genres 
    public static final String[] GENRE_LIST = new String[] { "Architecture", "Artifacts", "Bibliography", "Collection",
        "Criticism", "Drama", "Education", "Ephemera", "Fiction", "History", "Leisure", "Letters", "Life Writing",
        "Manuscript", "Music", "Nonfiction", "Paratext", "Periodical", "Philosophy", "Photograph", "Poetry",
        "Politics", "Religion", "Review", "Science", "Translation", "Travel", "Visual Art", "Citation", "Book History",
        "Family Life", "Folklore", "Humor", "Law", "Reference Works", "Sermon" };

    // Fields that are required to be present in RDF
    public static final String[] REQUIRED_FIELDS = new String[] { "archive", "title", "genre", "year", "freeculture",
        "has_full_text", "is_ocr", "federation", "url" };

    // Parallel to required fields - the actual tag name to be used
    // Any change above must be reflected hereå
    private static final String[] RDF_TERM = new String[] { "collex:archive", "dc:title", "collex:genre", "dc:date",
        "collex:freeculture", "collex:full_text", "collex:is_ocr", "collex:federation", "rdfs:seeAlso" };

    public static ArrayList<String> validateObject(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        messages.addAll(ValidationUtility.validateRequired(object));
        messages.addAll(ValidationUtility.validateGenre(object));
        messages.addAll(ValidationUtility.validateRole(object));
        messages.addAll(ValidationUtility.validateUri(object));

        return messages;
    }

    public static ArrayList<String> validateRole(HashMap<String, ArrayList<String>> object) {
        ArrayList<String> messages = new ArrayList<String>();

        // look for all role_* keys in object, validate that they are in this list:
        //    ART, AUT, EDT, PBL, and TRL, also CRE, EGR, ETR

        Set<String> keys = object.keySet();
        for (String key : keys) {
            if (key.startsWith("role_")
                && !("role_ART".equals(key) || "role_AUT".equals(key) || "role_EDT".equals(key)
                    || "role_PBL".equals(key) || "role_CRE".equals(key) || "role_EGR".equals(key)
                    || "role_ETR".equals(key) || "role_TRL".equals(key))) {
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
                        messages.add(genre + " genre not approved by NINES");
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

    public static void populateTextField(Element field, String text) {
        // first, try it without sanitizing, only sanitize if there is a problem with the text.
        try {
            field.setText(text);
        } catch (IllegalDataException e) {
            field.setText(filterNonXMLCharacters(text));
        }
    }

    public static String filterNonXMLCharacters(String text) {
        char[] outputString = new char[text.length()];

        int j = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Verifier.isXMLCharacter(c)) {
                outputString[j++] = c;
            }
        }

        return String.copyValueOf(outputString, 0, j);
    }

}
