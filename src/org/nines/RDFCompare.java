package org.nines;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.jaxen.JaxenException;
import org.jaxen.jdom.JDOMXPath;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;
import org.jdom.xpath.XPath;
import org.nines.RDFIndexerConfig.CompareMode;

/**
 * RDF Compare will perform comparisions on the target arcive and the main SOLR index.
 * 
 * @author loufoster
 *
 */
public class RDFCompare {
  
  private RDFIndexerConfig config;
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
    List<Element> indexDocs = getAllPagesFromArchive(
        "resources", config.archiveName, fl);
    logInfo("retrieved " + indexDocs.size() +" old rdf objects;");
    
    // get docs from reindex archive
    List<Element> archiveDocs = getAllPagesFromArchive(archiveToCoreName(
        config.archiveName), config.archiveName, fl);
    logInfo("retrieved "+archiveDocs.size()+" new rdf objects;");
    
    // do the comparison
    try {
      compareLists(indexDocs, archiveDocs);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    try {
    for (Map.Entry<String, List<String>> entry: this.errors.entrySet()) {
      String uri = entry.getKey();
      this.log.info("---"+uri+"---");
      for (String msg : entry.getValue() ) {
        this.log.info("    "+msg);
      }
    }
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    // done log some stats
    this.log.info("Total Docs Scanned: "+archiveDocs.size()+". Total Errors: "+this.errorCount+". Total Docs in index: "+indexDocs.size());
    
    Date end = new Date();
    double durationSec = (end.getTime()-start.getTime())/1000.0;
    if (durationSec >= 60 ) {
      this.log.info( String.format("Finished in %3.2f minutes.", (durationSec/60.0)));
    } else {
      this.log.info( String.format("Finished in %3.2f seconds.", durationSec));
    }

  }


  private String getFieldList() {
    
    if ( this.config.compareMode.equals(CompareMode.FAST)) {
      StringBuilder sb = new StringBuilder();
      for (String s : NON_TEXT_FIELDS) {
        if (sb.length()>0) {
          sb.append("+");
        }
        sb.append(s);
      }
      return sb.toString();
    } else if ( this.config.compareMode.equals(CompareMode.TEXT)) {
      return "uri+text";
    }
    return "*";
  }
  
  /**
   * Scan thru each document in the archive and find differences 
   * @param indexDocs List of all original docs in the index
   * @param archiveDocs List of docs in the reindexed archive
   * @throws Exception
   */
  private void compareLists(List<Element> indexDocs, List<Element> archiveDocs) throws Exception {
    
    // hash the indexed docs by uri to hopefull speed stuff up
    HashMap<String, Element> indexed = new HashMap<String, Element>();
    JDOMXPath xp = new JDOMXPath("str[@name='uri']");
    for ( Element ele : indexDocs) {
      Element uriEle = (Element) xp.selectSingleNode(ele);
      String uri = uriEle.getText();
      indexed.put(uri, ele);
    }
    indexDocs.clear();
    
    // Run thru al items in new archive. Validate correct data
    // and compare against object in original index if possible
    for ( Element doc : archiveDocs) {
      
      // validaate all required data is present in new rev
      Element uriEle = (Element) xp.selectSingleNode(doc);
      String uri = uriEle.getText();
      validateRequiredFields(uri, doc);
      
      // look up the object in existing index
      Element indexDoc = indexed.get(uri);
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
   * @param newDoc
   * @throws JaxenException
   */
  @SuppressWarnings("unchecked")
  private void compareFields(String uri, Element indexDoc, Element newDoc) throws JaxenException {
    
    List<Element> eles = newDoc.getChildren();
    for ( Element ele : eles ) {
      
      // grab new val
      String newVal = getValue( ele );
      String key = ele.getAttributeValue("name");
      //System.out.println("NEW NAME: "+key+" VAL: "+newVal);
      
      // use xpath to find the same field in the indexDoc
      JDOMXPath xp = new JDOMXPath("*[@name='" + key + "']");
      Element oldEle = (Element) xp.selectSingleNode(indexDoc);
      String oldVal = getValue( oldEle );
      //System.out.println("OLD NAME: "+key+" VAL: "+oldVal);
       
      // New key/value?
      if (oldVal == null) {
        if (isIgnoredNewField(key) == false) {
          addError( uri, key+" "+newVal.replaceAll("\n", " / ")+" introduced in reindexing.");
        }
        continue;
      }
      
      // dump the old ele so we can later detect leftover fields
      indexDoc.getChildren().remove(oldEle);
      
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
    eles = indexDoc.getChildren();
    for ( Element ele : eles ) {
      String val = getValue( ele );
      String key = ele.getAttributeValue("name");
      if ( val.length() > 100) {
        val = val.substring(0,100);
      }
      addError(uri, "Key not reindexed: "+key+"="+val);
    }
  }
  
  private String getProcessedOrigFied(String origVal) {
    String val = StringEscapeUtils.escapeXml(origVal);
    val = val.replaceAll("\n", " ");
    return removeExtraWhiteSpace(val);
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
   * Extract the value of the element. If the element is
   * and array, the value is all child values joined by |
   * 
   * @param ele
   * @return String content of the element, or null if element is invaid
   */
  private String getValue(Element ele) {
    if ( ele == null) {
      return null;
    }
    if ( ele.getName().equalsIgnoreCase("arr")) {
      @SuppressWarnings("unchecked")
      List<Element> children = ele.getChildren();
      String childTxt = "";
      for ( Element child : children ) {
        if ( childTxt.length() > 0) {
          childTxt += " | ";
        }
        childTxt += child.getText().trim();
      }
      return childTxt;
    }
    
    return ele.getText().trim();
  }

  /**
   * Ensure that all required fields are present and contain data
   * @param uri URI of the document
   * @param doc Document XML data
   * @throws Exception
   */
  private void validateRequiredFields(String uri, Element doc)  {

    for ( String field : REQUIRED_FIELDS) {

      // find the first element in the correct doc that
      // has a name attribute matching the  required field
      Element fieldEle = null;
      try {
        
        JDOMXPath xp = new JDOMXPath("*[@name='" + field + "']");
        fieldEle = (Element) xp.selectSingleNode(doc);
      } catch ( JaxenException e) {
        
        addError(uri, "XPath to required field: "+field
            +" threw exception: "+e.getMessage());
        continue;
      }
      
      // make sure field is present
      if ( fieldEle == null ) {
        
        addError(uri, "required field: "+field+" missing in new index");
        
      } else {
        
        // if its an array, make sure it has children
        // and that the concatenated children content has length
        if ( fieldEle.getName().equalsIgnoreCase("arr")) {
          
          @SuppressWarnings("unchecked")
          List<Element> children = fieldEle.getChildren();
          if (children.size() == 0) {
            addError(uri, "required ARR field: "+field+" is NIL in new index");
          } else {
            String childTxt = "";
            for ( Element child : children ) {
              childTxt += child.getText();
            }
            if ( childTxt.trim().length() == 0) {
              addError(uri, "required ARR field: "+field+" is all spaces in new index");
            }
          }
        } else if ( fieldEle.getName().equalsIgnoreCase("str")) {
          
          // make sure string elements have data
          if ( fieldEle.getText().trim().length() == 0) {
            addError(uri, "required STR field: "+field+" is all spaces in new index");
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
    //System.out.println(msg);
  }
  
  /**
   * Generate a clean core name from an archive
   * @param archive
   * @return
   */
  private String archiveToCoreName( final String archive) {
    return "archive_"+archive.replace(":", "_").replace(" ", "_").replace(",", "_");
  }

  private List<Element> getAllPagesFromArchive(final String core, final String archive, final String fields) {
    int page = 0;
    int size = 500;
    List<Element> results = new ArrayList<Element>();
    while (true) {
     
      try {
        
        List<Element> pageHits = getPageFromSolr(core, archive, page, size, fields);
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
  
  @SuppressWarnings("unchecked")
  private List<Element> getPageFromSolr(final String core, final String archive, 
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
      
      try {
        
        JavaBinCodec jbc = new JavaBinCodec();
        SimpleOrderedMap map = (SimpleOrderedMap) jbc.unmarshal( get.getResponseBodyAsStream() );
        
        //avaBin
        String response = RDFIndexer.getResponseString(get);
        SAXBuilder sb = new SAXBuilder();
        Document doc = sb.build(new StringReader(response) );
        JDOMXPath xPath = new JDOMXPath("//doc");
        return xPath.selectNodes(doc);
        
      } catch ( Exception e) {
        throw new IOException( "Invalid response", e );
      }
      
    } finally {
      // Release current connection to the connection pool once you are done
      get.releaseConnection();
    }
  }
}
