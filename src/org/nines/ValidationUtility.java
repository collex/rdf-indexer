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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Set;

import org.jdom.Element;
import org.jdom.IllegalDataException;
import org.jdom.Verifier;

public class ValidationUtility {

  public static boolean validateTextField( final String txt ) {
  
    int startPos = 0;
    while (true) {
      int pos = txt.indexOf("&#", startPos);
      if ( pos == -1) {
        break;
      } else {
        int p2 = txt.indexOf(";", pos);
        if ( p2 == -1 ) {
          return false;
        }
        
        String data = txt.substring(pos+2,p2);
        int intVal = Integer.parseInt(data);
        byte[] bytes = intToByteArray(intVal);
        try {
          CharBuffer  cb = Charset.availableCharsets().get("UTF-8").newDecoder()
            .decode(ByteBuffer.wrap(bytes));
          String outVal = cb.toString();
          System.out.println("VAL ["+outVal+"]");
        } catch (CharacterCodingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          return false;
        }
        startPos = p2+1;
      }
    }
    return true;
  }
  
  private static final byte[] intToByteArray(int value) {
    byte[] b = new byte[4];
    for (int i = 0; i < 4; i++) {
      int offset = (b.length - 1 - i) * 8;
      b[i] = (byte) ((value >>> offset) & 0xFF);
    }
    return b;
  }
  
  
  public static boolean validateObjects(HashMap<String, HashMap<String, ArrayList<String>>> objects, String filename) {
    Set<String> keys = objects.keySet();
    for (String uri : keys) {
      HashMap<String, ArrayList<String>> object = objects.get(uri);
      validateObject(object);
    }

    return true;
  }

  public static ArrayList<ErrorMessage> validateObject(HashMap<String, ArrayList<String>> object) {
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();

    messages.addAll(validateRequired(object));
    messages.addAll(validateGenre(object));
    messages.addAll(validateRole(object));
    messages.addAll(validateUri(object));

    return messages;
  }

  public static ArrayList<ErrorMessage> validateRole(HashMap<String, ArrayList<String>> object) {
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();

    // look for all role_* keys in object, validate that they are in this list:
    //    ART, AUT, EDT, PBL, and TRL, also CRE, EGR, ETR

    Set<String> keys = object.keySet();
    for (String key : keys) {
      if (key.startsWith("role_") &&
           !("role_ART".equals(key) ||
             "role_AUT".equals(key) ||
             "role_EDT".equals(key) ||
             "role_PBL".equals(key) ||
             "role_CRE".equals(key) ||
             "role_EGR".equals(key) ||
             "role_ETR".equals(key) ||
             "role_TRL".equals(key))) {
        messages.add(new ErrorMessage(false, "invalid role: " + key));
      }
    }

    return messages;
  }

  public static ArrayList<ErrorMessage> validateUri(HashMap<String, ArrayList<String>> object) {
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();

	// The URI can't contain foo
    ArrayList<String> fields = object.get("uri");
	if (fields.size() > 1)
        messages.add(new ErrorMessage(false, "must contain exactly one URI field"));
	if (!fields.isEmpty()) {
	    String fieldVal = fields.get(0);
		if (fieldVal.startsWith("http://foo/"))
	        messages.add(new ErrorMessage(false, "URI field is not created properly"));
	}

    return messages;
  }

  /**
   * Confirms that required fields are present and non-null
   */
  public static ArrayList<ErrorMessage> validateRequired(HashMap<String, ArrayList<String>> object) {
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();
    String[] requiredFields = new String[]{"archive", "title", "genre", "year", "freeculture", "has_full_text", "is_ocr", "federation", "url" };
    String[] rdfTerm = new String[]{"collex:archive", "dc:title", "collex:genre", "dc:date", "collex:freeculture", "collex:full_text", "collex:is_ocr", "collex:federation", "rdfs:seeAlso"};

    for (int i = 0; i < requiredFields.length; i++) {
      if (!object.containsKey(requiredFields[i])) {
        messages.add(new ErrorMessage(false, "object must contain the " +
            rdfTerm[i] + " field"));
      }
    }

    ArrayList<String> fields = object.get("archive");
	if (fields.size() > 1)
        messages.add(new ErrorMessage(false, "must contain exactly one archive field"));

    Set<String> keys = object.keySet();
    boolean hasRole = false;
    for (String key : keys) {
      if (key.startsWith("role_")) {
        hasRole = true;
        break;
      }
    }

    if (!hasRole) {
      messages.add(new ErrorMessage(false, "object must contain at least one role:XXX field"));
    }

    return messages;
  }

  /**
   * The genre must be in a constrained list.
   */
  public static ArrayList<ErrorMessage> validateGenre(HashMap<String, ArrayList<String>> object) {
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();

    Set<String> keys = object.keySet();
    for (String field : keys) {
      if ("genre".equals(field)) {
        // test 1: each genre is valid
        ArrayList<String> valueList = object.get(field);
        ListIterator<String> lit = valueList.listIterator();

        while (lit.hasNext()) {
          String genre = lit.next();
          if (!validateGenreInList(genre)) {
            messages.add(new ErrorMessage(false,
                genre + " genre not approved by NINES"));
          }
        }
      }
    }

    return messages;
  }

  public static boolean validateGenreInList(String genre) {
    String[] genreList = new String[]{"Architecture",
        "Artifacts", "Bibliography",
        "Collection", "Criticism", "Drama",
        "Education", "Ephemera", "Fiction",
        "History", "Leisure", "Letters",
        "Life Writing", "Manuscript", "Music",
        "Nonfiction", "Paratext", "Periodical",
        "Philosophy", "Photograph", "Poetry", "Politics",
        "Religion", "Review", "Science",
        "Translation", "Travel",
        "Visual Art", "Citation",
        "Book History", "Family Life", "Folklore",
        "Humor", "Law", "Reference Works", "Sermon"
    };

    for (String aGenreList : genreList) {
      if (aGenreList.equals(genre)) {
        return true;
      }
    }

    return false;
  }

  public static ArrayList<ErrorMessage> validateFreecultureElement(HashMap<String, ArrayList<String>> object) {
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();

    ArrayList<String> fields = object.get("freeculture");
    String fieldVal = fields.get(0);
    if (!validateFreeculture(fieldVal)) {
      messages.add(new ErrorMessage(false,
          fieldVal + " is not a valid value for collex:freeculture"));
    }

    return messages;
  }

  public static boolean validateFreeculture(String value) {
    if (value == null || "true".equals(value) || "false".equals(value))
      return true;
    else
      return false;
  }
  
  public static void populateTextField( Element field, String text ) {
	  // first, try it without sanitizing, only sanitize if there is a problem with the text.
	  try {
		  field.setText(text);		  
	  }
	  catch( IllegalDataException e ) {
		  field.setText(filterNonXMLCharacters(text));
	  }
  }
  
  public static String filterNonXMLCharacters( String text ) {	  
	  char[] outputString = new char[ text.length() ];
	  
	  int j = 0;
	  for( int i = 0; i < text.length(); i++ ) {
		  char c = text.charAt(i);
		  if( Verifier.isXMLCharacter(c) ) {
			  outputString[j++] = c;
		  }		  
	  }
	  
	  return String.copyValueOf(outputString, 0, j);
  }
    
}
