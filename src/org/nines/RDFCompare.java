package org.nines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.nines.RDFIndexerConfig.CompareMode;

/**
 * RDF Compare will perform comparisions on the target arcive and the main SOLR index.
 * 
 * @author loufoster
 *
 */
public class RDFCompare {
  
  private RDFIndexerConfig config;
  private boolean includesText = false;
  private Logger log;
  private HttpClient httpClient;
  private LinkedHashMap<String,List<String>> errors = new LinkedHashMap<String,List<String>>();
  private int errorCount = 0;
  
  // all of the solr instance fields. Text is the last field
  private static final ArrayList<String> NON_TEXT_FIELDS = new ArrayList<String>( Arrays.asList(
    "uri", "archive", "date_label", "genre", "source", "image", "thumbnail", "title", 
    "alternative", "url", "role_ART", "role_AUT", "role_EDT", "role_PBL", "role_TRL", 
    "role_EGR", "role_ETR", "role_CRE", "freeculture", "is_ocr", "federation", 
    "has_full_text", "source_xml", "typewright", "publisher", "agent", "agent_facet", 
    "author", "batch", "editor", "text_url", "year", "type", "date_updated", "title_sort", 
    "author_sort", "year_sort", "source_html", "source_sgml", "person", "format", 
    "language", "geospacial"));

  private static final ArrayList<String> LARGE_TEXT_ARCHIVES = new ArrayList<String>( Arrays.asList(
      "PQCh-EAF", "amdeveryday", "oldBailey" ));
  
  private static final ArrayList<String> REQUIRED_FIELDS = new ArrayList<String>( Arrays.asList(
      "title_sort", "title", "genre", "archive", "url", 
      "federation", "year_sort", "freeculture", "is_ocr"));
  
  // Static connecton config
  private static final int SOLR_REQUEST_NUM_RETRIES = 5; // how many times we should try to connect with solr before giving up
  private static final int SOLR_REQUEST_RETRY_INTERVAL = 30 * 1000; // milliseconds
  private static final int HTTP_CLIENT_TIMEOUT = 2*60*1000; // 2 minutes


  /**
   * Construct an instance of the RDFCompare with the specified config
   * @param config
   * @throws IOException 
   */
  public RDFCompare(RDFIndexerConfig config) throws IOException {
    this.config = config;
    
    // init logging
    String logFileName = config.archiveName;
    logFileName = logFileName.replaceAll("/", "_").replaceAll(":", "_").replaceAll(" ", "_");
    String logFileRelativePath = "../../../log/";
    String logPath = logFileRelativePath+logFileName + "_compare_"+ config.compareMode+".log";
    System.out.println("Log to "+logPath.toLowerCase());
    FileAppender fa = new FileAppender(new PatternLayout("%m\n"), logPath.toLowerCase());
    BasicConfigurator.configure( fa );
    this.log = Logger.getLogger(RDFIndexer.class.getName());
    
    // init the solr connection
    this.httpClient = new HttpClient();
    this.httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(
        HTTP_CLIENT_TIMEOUT);
    this.httpClient.getHttpConnectionManager().getParams().setIntParameter(
        HttpMethodParams.BUFFER_WARN_TRIGGER_LIMIT, 10000 * 1024);
  }


  /**
   * Perform the comparison based on the config passed into the c'tor
   */
  public void compareArhive() {

    // log start time
    Date start = new Date();
    this.log.info("Started compare at " + start);
    logInfo("====== Scanning archive \"" + config.archiveName + "\" ====== ");
    
    String fl = getFieldList();
    
    // get docs from current index
    List<SolrDocument> indexDocs = getAllPagesFromArchive(
        "resources", config.archiveName, fl);
    logInfo("retrieved " + indexDocs.size() +" old rdf objects;");
    
    // get docs from reindex archive
    List<SolrDocument> archiveDocs = getAllPagesFromArchive(archiveToCoreName(
        config.archiveName), config.archiveName, fl);
    logInfo("retrieved "+archiveDocs.size()+" new rdf objects;");
    
    // do the comparison
    compareLists(indexDocs, archiveDocs);
    
    for (Map.Entry<String, List<String>> entry: this.errors.entrySet()) {
      String uri = entry.getKey();
      logInfo("---"+uri+"---");
      for (String msg : entry.getValue() ) {
        logInfo("    "+msg);
      }
    }
    
    // done log some stats
    this.log.info("Total Docs Scanned: "+archiveDocs.size()+". Total Errors: "+this.errorCount+". Total Docs in index: "+indexDocs.size());
    
    Date end = new Date();
    double durationSec = (end.getTime()-start.getTime())/1000.0;
    if (durationSec >= 60 ) {
      logInfo( String.format("Finished in %3.2f minutes.", (durationSec/60.0)));
    } else {
      logInfo( String.format("Finished in %3.2f seconds.", durationSec));
    }
  }

  private String getFieldList() {
    
    this.includesText = true;
    if ( this.config.compareMode.equals(CompareMode.FAST)) {
      this.includesText = false;
      StringBuilder sb = new StringBuilder();
      for (String s : NON_TEXT_FIELDS) {
        if (sb.length()>0) {
          sb.append("+");
        }
        sb.append(s);
      }
      return sb.toString();
    } else if ( this.config.compareMode.equals(CompareMode.TEXT)) {
      return "uri+is_ocr+has_full_text+text";
    }
    return "*";
  }
  
  /**
   * Scan thru each document in the archive and find differences 
   * @param indexDocs List of all original docs in the index
   * @param archiveDocs List of docs in the reindexed archive
   * @throws Exception
   */
  private void compareLists(List<SolrDocument> indexDocs, List<SolrDocument> archiveDocs) {
    
    // hash the indexed docs by uri to hopefull speed stuff up
    HashMap<String, SolrDocument> indexed = new HashMap<String, SolrDocument>();
    for ( SolrDocument doc : indexDocs) {
      String uri = doc.get("uri").toString();
      indexed.put(uri, doc);
    }
    indexDocs.clear();
    
    // Run thru al items in new archive. Validate correct data
    // and compare against object in original index if possible
    for ( SolrDocument doc : archiveDocs) {
      
      // validaate all required data is present in new rev
      String uri = doc.get("uri").toString();
      validateRequiredFields(doc);
      
      // look up the object in existing index
      SolrDocument indexDoc = indexed.get(uri);
      if ( indexDoc != null) {
        compareFields( uri, indexDoc, doc);
      }
    }
  }

  /**
   * Walk through each field in the new doc and compare it with the
   * old. Log any differences.
   * 
   * @param uri
   * @param indexDoc
   * @param doc
   */
  private void compareFields(String uri, SolrDocument indexDoc, SolrDocument doc) {
    
    // loop over all keys in doc
    for (Entry<String, Object> entry: doc.entrySet()) {
      
      // get key and do special handing for text fields
      String key = entry.getKey();
      if ( key.equals("text")) {
        compareText(uri, indexDoc, doc);
        continue;
      }
      
      // grab new val
      String newVal = entry.getValue().toString();
      
      // is this a new key?
      if ( indexDoc.containsKey(key) == false) {
        if (isIgnoredNewField(key) == false) {
          addError( uri, key+" "+newVal.replaceAll("\n", " / ")+" introduced in reindexing.");
        } 
        continue;
      }
      
      // get parallel val in indexDoc
      String oldVal = indexDoc.get(key).toString();
      
      // dump the key from indexDoc so we can detect
      // unindexed values later
      indexDoc.remove(key);
      
      // don't compare batch or score
      if ( key.equals("batch") || key.equals("score") ) {        
        continue;
      }
     
      // difference?
      if ( newVal.equals(oldVal) == false) {
        
        // make sure everything is escaped and check again.
        String escapedOrig = getProcessedOrigFied(oldVal);
        String escapedNew = StringEscapeUtils.escapeXml(newVal);
        if ( escapedNew.equals(escapedOrig) == false ) {
          
          // too long to dump in a single error line?
          if (oldVal.length() > 30) {
          
            // log a summary
            addError(uri, key
                + " mismatched: length= " + newVal.length()+" (new)"
                + " vs. "+oldVal.length()+" (old)");
            
            // then find first diff and log it
            String[] oldArray = oldVal.split("\n");
            String[] newArray = newVal.split("\n");
            for ( int i=0; i<= oldArray.length; i++ ) {
              if ( oldArray[i].equals(newArray[i]) == false) {
               
                addError(uri, "        at line "+i+":\n"
                    + "\"" + newArray[i].replaceAll("\n", " / ") + "\" vs.\n" 
                    + "\"" + oldArray[i].replaceAll("\n", " / ") + "\"");
                break;
              }
            }

          } else {
            
            // dump the entire diff to the log
            addError(uri, key
                + " mismatched: \"" + newVal.replaceAll("\n", " / ") + "\" (new)" 
                + " vs. \"" + oldVal.replaceAll("\n", " / ") + "\" (old)");
          }

        }
      }
    }
    
    // now see if there are any leftover fields in indexDoc
    // log them is not reindexed
    for (Entry<String, Object> entry: indexDoc.entrySet()) {
      String val = entry.getValue().toString();
      String key = entry.getKey();
      if ( val.length() > 100) {
        val = val.substring(0,100);
      }
      addError(uri, "Key not reindexed: "+key+"="+val);
    }
  }
  
  /**
   * Compare just the TEXT field of the index and archive docs
   * @param uri
   * @param indexDoc
   * @param doc
   */
  private void compareText(String uri, SolrDocument indexDoc, SolrDocument doc) {
    
    Object newTxtObj = doc.get("text");
    Object oldTxtObj = indexDoc.get("text");
    indexDoc.remove("text");
    
    String newTxt = getTextFromObject(uri, "new", newTxtObj);
    String oldTxt = getTextFromObject(uri, "old", oldTxtObj);
    
    // log additional errors if no new text and doc is flagged
    // such that it must have text (ocr or full text)
    if (newTxt == null) {
      String val = doc.get("has_full_text").toString();
      if ( val.equalsIgnoreCase("false")) {
        addError(uri, "field has_full_text is "+val+" but full text does not exist.");
      }
      
      val = doc.get("is_ocr").toString();
      if ( val.equalsIgnoreCase("false")) {
        addError(uri, "field is_ocr is "+val+" but full text does not exist.");
      }
    }
    
    if (newTxt == null && oldTxt != null) {
      addError(uri, "text field has disappeared from the new index. (old text size = "+oldTxt.length());
    } else if (newTxt != null && oldTxt == null) {
      addError(uri, "text field has appeared in the new index.");
    } else if (newTxt.equals(oldTxt) == false) {
    
      newTxt = getProcessedReindexedText(newTxt);
      oldTxt = getProcessedOrigText(oldTxt);
      if (oldTxt.equals(newTxt) == false ) {
        logMismatchedText(uri, oldTxt, newTxt);
      }
    }    
  }
  
  private void logMismatchedText(final String uri, final String oldTxt, final String newTxt) {
    String[] oldLines = oldTxt.split("\n");
    String[] newLines = newTxt.split("\n");
    int firstMismatch = -1;
    for (int i=0; i<Math.min(oldLines.length, newLines.length); i++) {
      String oldLine = oldLines[i];
      String newLine = newLines[i];
      if ( oldLine.equals(newLine) == false ) {
        firstMismatch = i;
        break;
      }
    }
    
    // no mosmatch, but new is bigger than old, mismatch startsat end of old
    if (firstMismatch == -1 && newLines.length > oldLines.length) {
      firstMismatch = oldLines.length;
    }
    
    // still no mismatch, just bail
    if (firstMismatch == -1) {
      return;
    }
  
    String newLine = newLines[firstMismatch];
    String oldLine = oldLines[firstMismatch];
    int pos = StringUtils.indexOfDifference(newLine, oldLine);
    String newSub = newLine.substring(pos, Math.min(pos+50, newLine.length()));
    String oldSub = oldLine.substring(pos, Math.min(pos+50, oldLine.length()));
    addError(uri, "==== "+uri+" mismatch at line "+firstMismatch+":col "+pos+":\n(new "+newTxt.length()+")");
    addError(uri, newSub);
    addError(uri, "-- vs --\n(old "+oldLine.length()+")");
    addError(uri, oldSub);
    
    // generate hex string of new text
    byte[] bytes = newSub.getBytes();
    StringBuffer hexStr = new StringBuffer();
    for (int i=0; i<bytes.length; i++ ) {
      hexStr.append(Integer.toHexString(0xFF & bytes[i]) ).append(" ");
    }
    addError(uri, "NEW: "+hexStr.toString().trim());
    
    // generate hex of old bytes
    bytes = oldSub.getBytes();
    hexStr = new StringBuffer();
    for (int i=0; i<bytes.length; i++ ) {
      hexStr.append(Integer.toHexString(0xFF & bytes[i]) ).append(" ");
    }
    addError(uri, "OLD: "+hexStr.toString().trim());   
  }


  private String getTextFromObject(String uri, String prefix, Object txtObj) {
    if ( txtObj == null) {
      return null;
    }
    
    if ( txtObj instanceof List ) {
      @SuppressWarnings("unchecked")
      List<String> dat = (List<String>)txtObj;
      addError(uri, prefix+" text is an array of size "+dat.size());
      StringBuffer sb = new StringBuffer();
      for (String s: dat) {
        if (sb.length() > 0) {
          sb.append(" | ");
        }
        sb.append( s);
      }
      return sb.toString();
    } else {
      return txtObj.toString().trim();
    }  
  }
 

  private String getProcessedOrigFied(String origVal) {
    String val = StringEscapeUtils.escapeXml(origVal);
    val = val.replaceAll("\n", " ");
    return removeExtraWhiteSpace(val);
  }
  
  private String getProcessedOrigText(String origTxt) {
    String val = StringEscapeUtils.escapeXml(origTxt);
    val = val.replaceAll("\n", " ");
    val = val.replaceAll("““", "“");
    val = val.replaceAll("””", "””");
    val = val.replaceAll("††", "†");
    val = val.replaceAll("〉〉", "〉");
    val = val.replaceAll("\\—+", "—");
    return removeExtraWhiteSpace(val);
  }
  
  private String getProcessedReindexedText(String srcTxt ) {
    return removeExtraWhiteSpace(srcTxt);
  }
  
  private String removeExtraWhiteSpace(final String srcTxt) {
    String result = srcTxt.replaceAll("\t", " ");   // change tabs to spaces
    result = result.replaceAll("\\s+", " ");       // get rid of multiple spaces
    result = result.replaceAll(" \n", "\n");        // get rid of trailing spaces
    result = result.replaceAll("\n ", "\n");        // get rid of leading spaces
    result = result.replaceAll("\\n+", "\n");        // get rid of blank lines
    return result;
  }


  /**
   * EXCEPTION case. Dont whine about fields we know are newly added
   * @param key
   * @return
   */
  private boolean isIgnoredNewField(String key) {
    if (key.equals("year_sort") || 
        key.equals("has_full_text") ||
        key.equals("freeculture") ||
        key.equals("is_ocr") ||
        key.equals("author_sort") ) {
      return true;
    }
    return false;
  }

  private void addError(String uri, String err) {
    if ( this.errors.containsKey(uri) == false) {
      this.errors.put(uri, new ArrayList<String>() );
    }
    this.errors.get(uri).add(0,err);
    errorCount++;
  }

  /**
   * Ensure that all required fields are present and contain data
   * @param uri URI of the document
   * @param doc Document XML data
   * @throws Exception
   */
  private void validateRequiredFields(SolrDocument doc)  {

    for ( String fieldName : REQUIRED_FIELDS) {

      // find the first element in the correct doc that
      // has a name attribute matching the  required field
      String uri = doc.get("uri").toString();
      Object docField = doc.get(fieldName);
      
      // make sure field is present
      if ( docField == null ) {
        
        addError(uri, "required field: "+fieldName+" missing in new index");
        
      } else {
        
        // if its an array, make sure it has children
        // and that the concatenated children content has length
        if ( docField instanceof List ) {
          @SuppressWarnings("unchecked")
          List<String> list = (List<String>)docField;
          String val = "";
          for ( String data: list) {
            val += data;
          }
          if (val.length() == 0) {
            addError(uri, "required ARR field: "+fieldName+" is all spaces in new index");
          }
        } else {
          if ( docField.toString().trim().length() == 0) {
            addError(uri, "required STR field: "+fieldName+" is all spaces in new index");
          }
        }
      }
    }
  }

  /**
   * Log data to file and System.out
   * @param msg
   */
  private void logInfo( final String msg) {
    log.info(msg);
    System.out.println(msg);
  }
  
  /**
   * Generate a clean core name from an archive
   * @param archive
   * @return
   */
  private String archiveToCoreName( final String archive) {
    return "archive_"+archive.replace(":", "_").replace(" ", "_").replace(",", "_");
  }

  private List<SolrDocument> getAllPagesFromArchive(final String core, final String archive, final String fields) {
    int page = 0;
    int size = 500;
    
    // When fieldlist includes test, and the archive is one that contains
    // large text fields, limit page size to 1
    if ( this.includesText && LARGE_TEXT_ARCHIVES.contains(archive)) {
      size = 1;
    }
    
    List<SolrDocument> results = new ArrayList<SolrDocument>();
    while (true) {
     
      try {
        
        List<SolrDocument> pageHits = getPageFromSolr(core, archive, page, size, fields);
        results.addAll(pageHits);
        if (pageHits.size()  < size ){
          
          break;
        } else {
          
          page += 1;
          System.out.print( "." );
        }
        
      } catch (IOException e) {
        System.err.println("Error retrieving data from solr:" + e.getMessage());
        e.printStackTrace();
      }
    }
    return results;
  }
  
  private List<SolrDocument> getPageFromSolr(final String core, final String archive, 
      final int page, final int pageSize, final String fields) throws IOException {

    // build the request query string
    String query = this.config.solrBaseURL + "/" + core + "/select/?q=archive:"+archive;
    query = query + "&start="+(page*pageSize)+"&rows="+pageSize;
    query = query + "&fl="+fields;
    query = query + "&sort=uri+asc";
    query = query + "&wt=javabin";
    GetMethod get = new GetMethod( query );

    // Execute request
    try {
      int result;
      int solrRequestNumRetries = SOLR_REQUEST_NUM_RETRIES;
      do {
        result = this.httpClient.executeMethod(get);
        if (result != 200) {
          try {
            Thread.sleep(SOLR_REQUEST_RETRY_INTERVAL);
            log.info(">>>> postToSolr error in archive " + archive + ": " + result + " (retrying...)");
          } catch (InterruptedException e) {
            log.info(">>>> Thread Interrupted");
          }
        } else {
          if (solrRequestNumRetries != SOLR_REQUEST_NUM_RETRIES)
            log.info(">>>> postToSolr: " + archive + ":  (succeeded!)");
        }
        solrRequestNumRetries--;
      } while (result != 200 && solrRequestNumRetries > 0);

      if (result != 200) {
        throw new IOException("Non-OK response: " + result + "\n" );
      }

      JavaBinCodec jbc = new JavaBinCodec();
      @SuppressWarnings("rawtypes")
      SimpleOrderedMap map = (SimpleOrderedMap)jbc.unmarshal( get.getResponseBodyAsStream() );
      SolrDocumentList docList = (SolrDocumentList)map.get("response");
      return docList;

    } finally {
      // Release current connection to the connection pool once you are done
      get.releaseConnection();
    }
  }
}
