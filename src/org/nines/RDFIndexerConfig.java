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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Configuration for the RDFFileIndexer
 * 
 * @author nicklaiacona
 */
public class RDFIndexerConfig {
  
  // Possible modes of index comparison
  public enum CompareMode {
    NONE, // noc omparision  
    FAST, // compare all but TEXT
    TEXT, // compare only text
    FULL  // compare everything
  };
  
  // Modes for handling docs with ecternal text
  public enum TextMode {
    SKIP,           // external text is ignored
    RETRIEVE_FULL,  // retrieve full text from web
    REINDEX_FULL    // pull full text from existing index
  };
  
  public String logRoot = ".";
  public File rdfSource;
  public String archiveName;
  public String solrBaseURL = "http://localhost:8983/solr";
  public String solrExistingIndex = "/resources";
  public boolean collectLinks = true;
  public TextMode textMode = TextMode.SKIP;
  public int maxDocsPerFolder = 99999999;
  public String ignoreFileName = "";
  public ArrayList<String> ignoreFolders = new ArrayList<String>();
  public String includeFileName = "";
  public ArrayList<String> includeFolders = new ArrayList<String>();
  public boolean commitToSolr = true;
  public CompareMode compareMode = CompareMode.NONE;
  public boolean deleteAll = false;
  
  /**
   * Called after all options are set. This will open the
   * include and ignore files and populate the lists for each
   * with the contents of the file
   */
  public void populateFileLists() {
    if ( this.includeFileName != null && this.includeFileName.length() > 0) {
      populateList( this.includeFileName, this.includeFolders );
    }
    if ( this.ignoreFileName != null && this.ignoreFileName.length() > 0) {
      populateList( this.ignoreFileName, this.ignoreFolders );
    }
  }

  /**
   * helper method to parse a list of filenames from a source file
   * into the specified array list.
   * 
   * @param fileName
   * @param fileList
   */
  private void populateList(String fileName, ArrayList<String> fileList) {
    try {
      FileInputStream fstream = new FileInputStream(fileName);
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String strLine;
      while ((strLine = br.readLine()) != null) {
        fileList.add( this.rdfSource + "/" + strLine);
      }
      in.close();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
    
  }
}
