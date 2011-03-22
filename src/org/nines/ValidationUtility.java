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
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.jdom.Element;
import org.jdom.IllegalDataException;
import org.jdom.Verifier;

public class ValidationUtility {
  
  // List of all valid genres 
  public static final String[] GENRE_LIST = new String[]{"Architecture",
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
  
  // Fields that are required to be present in RDF
  public static final String[] REQUIRED_FIELDS = new String[] { 
    "archive", "title", "genre", "year", "freeculture", 
    "has_full_text", "is_ocr", "federation", "url" };
  
  // Parallel to required fields - the actual tag name to be used
  // Any change above must be reflected hereå
  private static final String[] RDF_TERM = new String[] { 
    "collex:archive", "dc:title", "collex:genre", "dc:date", "collex:freeculture",
    "collex:full_text", "collex:is_ocr", "collex:federation", "rdfs:seeAlso" };

  /**
   * Parse the passed data for UTF-8 escape sequences. Validate that the content of each
   * results in a vailid UTF-8 character. All errors are pushed into and array
   * of error messages and resturned.
   * 
   * @param txt Source text string
   * @return An ArrayList of ErrorMessage objects
   */
  public static final ArrayList<ErrorMessage> validateTextField(final String txt) {

    int startPos = 0;
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();
    
    while (true) {

      // find start of escape sequence
      int pos = txt.indexOf("&#", startPos);
      if (pos == -1) {
        // when no more can be found, terminate
        break;
      } else {
        
        // make sure the end marker exists as well
        int p2 = txt.indexOf(";", pos);
        if (p2 == -1) {
          
          System.err.println("Unterminated UTF-8 escape sequence found at pos [" + pos + "]");
          messages.add(new ErrorMessage(false, "Unterminated UTF-8 escape sequence found at pos [" + pos + "]") );
          break;
        }

        // pull the numeric sequence data and convert it to a byte array
        String data = txt.substring(pos + 2, p2).trim();
        byte[] bytes = intToByteArray(data);
        try {

            Charset utf8_cs = Charset.availableCharsets().get("UTF-8");
           String s = new String(bytes, utf8_cs.name());
           System.out.println(s);
           byte[] b2 = s.getBytes(utf8_cs.name());
           System.out.println(b2);
           
          // attempt to encode this byte array to UTF-8
//          
//          CharsetDecoder decoder = utf8_cs.newDecoder();
//          ByteBuffer bb = ByteBuffer.wrap(bytes);
//          
//          CharBuffer cb = decoder.decode(bb);
//          String foo = cb.toString();
//          if ( foo.length() > 1){
//            throw new Exception("multi char");
//          }

        } catch (Exception e) {

          String sequence = txt.substring(pos, p2 + 1).trim();
          System.err.println("Invalid UTF-8 escape string [" + sequence + "] at pos ["+pos+"]");
          messages.add(new ErrorMessage(false, "Invalid UTF-8 escape string [" + sequence + "] at pos ["+pos+"]") );
        }
        startPos = p2 + 1;
      }
    }
    
    return messages;
  }
  
  /**
   * Converts an array of Unicode scalar values (code points) into
   * UTF-8. This algorithm works under the assumption that all
   * surrogate pairs have already been converted into scalar code
   * point values within the argument.
   * 
   * @param ch an array of Unicode scalar values (code points)
   * @returns a byte[] containing the UTF-8 encoded characters
   */
  public static byte[] encode(int[] ch) {
    // determine how many bytes are needed for the complete conversion
    int bytesNeeded = 0;
    for (int i = 0; i < ch.length; i++) {
      if (ch[i] < 0x80) {
        ++bytesNeeded;
      } else if (ch[i] < 0x0800) {
        bytesNeeded += 2;
      } else if (ch[i] < 0x10000) {
        bytesNeeded += 3;
      } else {
        bytesNeeded += 4;
      }
    }
    // allocate a byte[] of the necessary size
    byte[] utf8 = new byte[bytesNeeded];
    // do the conversion from character code points to utf-8
    for (int i = 0, bytes = 0; i < ch.length; i++) {
      if (ch[i] < 0x80) {
        utf8[bytes++] = (byte) ch[i];
      } else if (ch[i] < 0x0800) {
        utf8[bytes++] = (byte) (ch[i] >> 6 | 0xC0);
        utf8[bytes++] = (byte) (ch[i] & 0x3F | 0x80);
      } else if (ch[i] < 0x10000) {
        utf8[bytes++] = (byte) (ch[i] >> 12 | 0xE0);
        utf8[bytes++] = (byte) (ch[i] >> 6 & 0x3F | 0x80);
        utf8[bytes++] = (byte) (ch[i] & 0x3F | 0x80);
      } else {
        utf8[bytes++] = (byte) (ch[i] >> 18 | 0xF0);
        utf8[bytes++] = (byte) (ch[i] >> 12 & 0x3F | 0x80);
        utf8[bytes++] = (byte) (ch[i] >> 6 & 0x3F | 0x80);
        utf8[bytes++] = (byte) (ch[i] & 0x3F | 0x80);
      }
    }
    return utf8;
  }
  
  private static final byte[] intToByteArray(final String numVal) {
    int val = 0;
    if (numVal.toLowerCase().charAt(0) == 'x') {
      val = Integer.parseInt(numVal.substring(1),16);
    } else {
      val = Integer.parseInt(numVal);
    }
    
    int[] vals = { val };
    byte[] b1 = encode(vals);
    return b1;
  }

  public static ArrayList<ErrorMessage> validateObject(HashMap<String, ArrayList<String>> object) {
    ArrayList<ErrorMessage> messages = new ArrayList<ErrorMessage>();

    messages.addAll( ValidationUtility.validateRequired(object) );
    messages.addAll( ValidationUtility.validateGenre(object));
    messages.addAll( ValidationUtility.validateRole(object));
    messages.addAll( ValidationUtility.validateUri(object));
    
//    boolean dump = false;
//    if (object.containsKey("text")) {
//      ArrayList< String > vals = object.get("text");
//      
//      ArrayList<String> t = object.get("title");
//      if (t.get(0).equals("The Man Overboard")) {
//        System.out.println("TITLE: "+t.get(0));
//        dump = true;
//      }
//      
//      
//      for ( String txt: vals) {
//        if ( dump ) {
//          System.out.println("PARTIAL TEXT: "+txt.substring(0,100));
//          int pos = txt.indexOf("&#");
//          if ( pos > -1) {
//            System.out.println("GOT ESCAPE SEQUENCE AT "+pos);
//          }
//        }
//        messages.addAll( ValidationUtility.validateTextField(txt));
//      }
//    }

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
    
    for (int i = 0; i < REQUIRED_FIELDS.length; i++) {
      if (!object.containsKey(REQUIRED_FIELDS[i])) {
        messages.add(new ErrorMessage(false, "object must contain the " + RDF_TERM[i] + " field"));
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

    for (Map.Entry<String, ArrayList<String>> entry: object.entrySet()) {
      
      String key = entry.getKey();
      ArrayList<String> valueList = entry.getValue();
      
      if ("genre".equals(key)) {
        // test 1: each genre is valid
        for ( String genre : valueList) {
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
 
    for (String aGenreList : GENRE_LIST) {
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
