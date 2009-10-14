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

/**
 * Configuration for the RDFFileIndexer
 * @author nicklaiacona
 */
public class RDFIndexerConfig {   
    public String solrBaseURL = "http://localhost:8983/solr";
	public String solrExistingIndex = "/resources";
	public String solrNewIndex = "/reindex_rdf";
    public boolean collectLinks = true;
    public boolean retrieveFullText = false;
	public boolean reindexFullText = false;
	public int maxDocsPerFolder = 99999999;
	public ArrayList< String > ignoreFolders = new ArrayList < String >();
	public ArrayList< String > includeFolders = new ArrayList < String >();
    public boolean commitToSolr = true;
}
