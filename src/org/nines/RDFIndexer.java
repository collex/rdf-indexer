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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.nines.RDFIndexerConfig.CompareMode;
import org.nines.RDFIndexerConfig.TextMode;

public class RDFIndexer {

  private int numFiles = 0;
  private int numObjects = 0;
  private String guid = "";
  private RDFIndexerConfig config;
  private ConcurrentLinkedQueue<File> dataFileQueue;
  private ErrorReport errorReport;
  private LinkCollector linkCollector;
  private int progressMilestone = 0;
  private long lastTime;
  private Logger log;

  private static final int SOLR_REQUEST_NUM_RETRIES = 5; // how many times we should try to connect with solr before giving up
  private static final int SOLR_REQUEST_RETRY_INTERVAL = 30 * 1000; // milliseconds
  public static final int HTTP_CLIENT_TIMEOUT = 2*60*1000; // 2 minutes
  private static final int NUMBER_OF_INDEXER_THREADS = 5;
  private static final int PROGRESS_MILESTONE_INTERVAL = 100;
  private static final int DOCUMENTS_PER_POST = 100;
  
  /**
   * 
   * @param rdfSource
   * @param archiveName
   * @param config
   */
  public RDFIndexer( RDFIndexerConfig config) {

    this.config = config;
    
    // Use the archive name as the log file name
    String reportFilename = config.archiveName;
    reportFilename = reportFilename.replaceAll("/", "_").replaceAll(":", "_").replaceAll(" ", "_");
    String logFileRelativePath = "../../../log/";
    initSystem(logFileRelativePath + reportFilename);

    // log text mode
    this.log.info(config.textMode == TextMode.RETRIEVE_FULL ? "Online: Indexing Full Text" : "Offline: Not Indexing Full Text");
    
    // log comparison mode
    if  (config.compareMode != CompareMode.NONE ) {
      this.log.info("Comparison Mode: " + config.compareMode);
    }
    
    // keep report file in the same  folder as the log file.
    File reportFile = new File(logFileRelativePath + reportFilename + "_error.log"); 
    try {
      this.errorReport = new ErrorReport(reportFile);
    } catch (IOException e1) {
      this.log.error("Unable to open error report log for writing, aborting indexer.");
      return;
    }

    this.linkCollector = new LinkCollector(logFileRelativePath + reportFilename);

    HttpClient client = new HttpClient();

    try {
      beSureCoreExists(client, archiveToCore(config.archiveName));
    } catch (IOException e) {
      this.errorReport.addError(new IndexerError("Creating core", "", e.getMessage()));
    }

    // do the indexing if requested
    if ( config.textMode != TextMode.SKIP ) {
      createGUID(config.rdfSource);
      Date start = new Date();
      log.info("Started indexing at " + start);

      indexDirectory(config.rdfSource);
      if (config.commitToSolr) {
        // for (String archive : archives)
        commitDocumentsToSolr(client, archiveToCore(config.archiveName));
      } else {
        log.info("Skipping Commit to SOLR...");
      }

      // report indexing time
      Date end = new Date();
      long duration = (end.getTime() - start.getTime()) / 60000;
      this.log.info("Indexed " + numFiles + " files (" + numObjects + " objects) in " + duration + " minutes");
      this.errorReport.close();
      this.linkCollector.close();
    }
    
    // do any testing if requested
    if  (config.compareMode != CompareMode.NONE ) {

      log.info("Staring " + config.compareMode + " comparison");
      try {
        RDFCompare rdfCompare = new RDFCompare( config );
        rdfCompare.compareArhive();
      } catch (Exception e) {
        System.err.println("Comparison exception: " + e.getMessage());
        e.printStackTrace();
        log.error("Comparison exception: " + e.getMessage());
        
      }
    }
  }
  
  private void commitDocumentsToSolr( HttpClient client, String archive ) {
    try {
        if (numObjects > 0) {
            postToSolr("<optimize waitFlush=\"true\" waitSearcher=\"true\"/>",client, archive);
            postToSolr("<commit/>",client, archive);
        }
    }
    catch( IOException e ) {
        errorReport.addError(new IndexerError("","","Unable to POST commit message to SOLR. "+e.getLocalizedMessage()));    		
    }
  }
  
  private void createGUID( File rdfSource ) {
      String path = rdfSource.getPath();
      String file = path.substring(path.lastIndexOf('/') + 1);

      try {
        guid = file.substring(0, file.indexOf('.'));
      } catch (StringIndexOutOfBoundsException e) {
        /*
           * In cases where the indexer is run manually against a directory that doesn't
           * specify the GUID, create one automatically.
           */
        guid = java.util.UUID.randomUUID().toString();
      }
  }

  private void initSystem(String logName) {
    // Use the SAX2-compliant Xerces parser:
    System.setProperty(
        "org.xml.sax.driver",
        "org.apache.xerces.parsers.SAXParser");
    
   
    try {
      // don't purge old log on startup -- that is handled before calling this app.
      String logPath = logName + "_progress.log";      
      FileAppender fa = new FileAppender(new PatternLayout("%d{E MMM dd, HH:mm:ss} [%p] - %m\n"), logPath);
      fa.setEncoding("UTF-8");
      BasicConfigurator.configure( fa );
      log = Logger.getLogger(RDFIndexer.class.getName());

    }
    catch( IOException e ) {
        log.error("Error, unable to initialize logging, exiting.");
        System.exit(0);
    }
      
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
  }
  
	private void recursivelyQueueFiles(File dir) {
		if (dir.isDirectory()) {
			log.info("loading directory: " + dir.getPath());
			int numFilesInFolder = 0;

			File fileList[] = dir.listFiles();
			for (File entry : fileList) {
				if (entry.isDirectory() && !entry.getName().endsWith(".svn")) {
					String path = entry.toString();
					if (config.ignoreFolders.size() > 0) {
						int index = config.ignoreFolders.indexOf(path);
						if (index == -1) {
							recursivelyQueueFiles(entry);
						}
					} else if (config.includeFolders.size() > 0) {
						int index = config.includeFolders.indexOf(path);
						if (index != -1) {
							recursivelyQueueFiles(entry);
						}
					} else {
						recursivelyQueueFiles(entry);
					}
				}
				if (entry.getName().endsWith(".rdf") || entry.getName().endsWith(".xml")) {
					numFilesInFolder++;
					if (numFilesInFolder < config.maxDocsPerFolder) {
						dataFileQueue.add(entry);
					}
				}
			}
		}
		else // a file was passed in, not a folder
		{
			log.info("loading file: " + dir.getPath());
			dataFileQueue.add(dir);
		}
	}
	
  private void indexDirectory(File rdfDir) {
    dataFileQueue = new ConcurrentLinkedQueue<File>();
    recursivelyQueueFiles(rdfDir);
    numFiles = dataFileQueue.size();
    
    log.info("=> Indexing " + rdfDir + " total files: " + numFiles);
    
    initSpeedReporting();

    // fire off the indexing threads
    ArrayList<IndexerThread> threads = new ArrayList<IndexerThread>(); 
    for( int i = 0; i < NUMBER_OF_INDEXER_THREADS; i++ ) {
        IndexerThread thread = new IndexerThread(i);
        threads.add(thread);
        thread.start();
    }
    
    waitForIndexingThreads(threads);
  }
  
  private void waitForIndexingThreads(ArrayList<IndexerThread> threads) {
    boolean done = false;
    while (!done) {

      // nap between checks
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {}

      // check status
      done = true;
      for (IndexerThread thread : threads) {
        done = done && thread.isDone();
      }
    }
  }

  /**
   * Generate a core name given the archive. The core
   * name is of the format: archive_[name]
   * 
   * @param archive
   * @return
   */
  private String archiveToCore(String archive) {
	  String core = archive.replaceAll(":", "_");
	  core = core.replaceAll(" ", "_");
	  //core = core.replaceAll("-", "_");
	  core = core.replaceAll(",", "_");
	  return "archive_" + core;
  }

  private void indexFile(File file, HttpClient client) {
    try {
      // Parse a file into a hashmap. 
      // Key is object URI, Value is a set of key-value pairs
      // that describe the object
      HashMap<String, HashMap<String, ArrayList<String>>> objects = RdfDocumentParser.parse(file, errorReport,
          linkCollector, config);

      // Log an error for no objects abd bail if size is zero
      if (objects.size() == 0) {
        errorReport.addError(new IndexerError(file.getName(), "", "No objects in this file."));
        errorReport.flush();
        return;
      }
      
      // XML doc containing rdf docs to be posted to solr
      Document solrDoc = new Document();
      Element root = new Element("add");
      solrDoc.addContent(root);
      int docCount = 0;
      
      String tgtArchive = "";
      XMLOutputter outputter = new XMLOutputter(Format.getCompactFormat());
      for (Map.Entry<String, HashMap<String, ArrayList<String>>> entry: objects.entrySet()) {
        
        String uri = entry.getKey();
        HashMap<String, ArrayList<String>> object = entry.getValue();
        
        // Validate archive and push objects intop new archive map
        ArrayList<String> objectArray = object.get("archive");
        if (objectArray != null) { 
          String objArchive = objectArray.get(0);
          tgtArchive = archiveToCore(objArchive);          
          if (!objArchive.equals(this.config.archiveName)) {
            this.errorReport.addError(new IndexerError(file.getName(), uri, "The wrong archive was found. "
                + objArchive + " should be " + this.config.archiveName));
          }
        } else {
          this.errorReport.addError(new IndexerError(file.getName(), uri, "Unable to determine archive for this object."));
        }

        // validate all other parts of object and generate error report
        try {
          ArrayList<ErrorMessage> messages = ValidationUtility.validateObject(object);
          for ( ErrorMessage message : messages ) {
            IndexerError e = new IndexerError(file.getName(), uri, message.getErrorMessage());
            errorReport.addError(e);
          }
        } catch (Exception valEx) {
          System.err.println("ERROR Validating file:" + file.getName() + " URI: " +  uri);
          valEx.printStackTrace();
          IndexerError e = new IndexerError(file.getName(), uri, valEx.getMessage());
          errorReport.addError(e);
        }
        
        // turn this object into an XML solr doc and add it to the xml post
        Element document = convertObjectToSolrDOM(uri, object);
        root.addContent(document);
        docCount++;
        
        // last stop validation checks. Take the dom and dump it to an xml
        // string. look for bad chars
        ArrayList<ErrorMessage> messages = ValidationUtility.validateSolrDOM( document );
        for ( ErrorMessage message : messages ) {
          IndexerError e = new IndexerError(file.getName(), uri, message.getErrorMessage());
          errorReport.addError(e);
        }

        // once threshold met, post all docs
        if ( docCount >= DOCUMENTS_PER_POST) {
          log.info("  posting:" + docCount + " documents to SOLR");
          postToSolr( outputter.outputString(solrDoc), client, tgtArchive);
          solrDoc = new Document();
          root = new Element("add");
          solrDoc.addContent(root);
          docCount = 0;
        }
      }
      
      // dump any remaining docs out to solr
      if ( docCount >= 0) {
        log.info("  posting:" + docCount + " documents to SOLR");
        postToSolr( outputter.outputString(solrDoc), client, tgtArchive);
      }

      numObjects += objects.size();
      reportIndexingSpeed(numObjects);

    } catch (IOException e) {
      errorReport.addError(new IndexerError(file.getName(), "", e.getMessage()));
    }
    errorReport.flush();
  }
  
  private void initSpeedReporting() {
	  lastTime = System.currentTimeMillis();
  }
  
  private void reportIndexingSpeed( int objectCount ) {  
    if( objectCount > (progressMilestone + PROGRESS_MILESTONE_INTERVAL) ) {
    	long currentTime = System.currentTimeMillis();
    	float rate = (float)(currentTime-lastTime)/(float)(objectCount-progressMilestone);
    	progressMilestone = objectCount;
    	lastTime = currentTime;
    	log.info("total objects indexed: "+numObjects+" current rate: "+rate+"ms per object."); 	
    }
  }

  private void beSureCoreExists(HttpClient httpclient, String core) throws IOException {
    GetMethod request = new GetMethod(config.solrBaseURL + "/admin/cores?action=STATUS");

    httpclient.getHttpConnectionManager().getParams().setConnectionTimeout(HTTP_CLIENT_TIMEOUT);
    httpclient.getHttpConnectionManager().getParams()
        .setIntParameter(HttpMethodParams.BUFFER_WARN_TRIGGER_LIMIT, 10000 * 1024);
    
    // Execute request
    try {
      int result;
      int solrRequestNumRetries = SOLR_REQUEST_NUM_RETRIES;
      do {
        result = httpclient.executeMethod(request);
        solrRequestNumRetries--;
        if (result != 200) {
          try {
            Thread.sleep(SOLR_REQUEST_RETRY_INTERVAL);
            log.info(">>>> postToSolr error: " + result + " (retrying...)");
            log.info(">>>> getting core information");
          } catch (InterruptedException e) {
            log.info(">>>> Thread Interrupted");
          }
        }
      } while (result != 200 && solrRequestNumRetries > 0);

      if (result != 200) {
        throw new IOException("Non-OK response: " + result + "\n\n");
      }
      String response = RDFIndexer.getResponseString( request );
      int exists = response.indexOf(">" + core + "<");
      if (exists <= 0) {
        // The core doesn't exist: create it.
        request = new GetMethod(config.solrBaseURL + "/admin/cores?action=CREATE&name=" + core + "&instanceDir=.");
        result = httpclient.executeMethod(request);
        log.info(">>>> Created core: " + core);
        try {
          Thread.sleep(SOLR_REQUEST_RETRY_INTERVAL);
        } catch (InterruptedException e) {
          log.info(">>>> Thread Interrupted");
        }
      }
    } finally {
      // Release current connection to the connection pool once you are done
      request.releaseConnection();
    }
  }

  /**
   * Get the response body string from the HttpMethod. Internally uses 
   * getResponseBodyAsStream.
   * 
   * @param httpMethod
   * @return The response body string
   * 
   * @throws IOException
   */
  public static final String getResponseString(HttpMethod httpMethod) throws IOException {
    InputStream is = httpMethod.getResponseBodyAsStream();
    return IOUtils.toString(is, "UTF-8");
  }

  public void postToSolr(String xml, HttpClient httpclient, String archive) throws IOException {

    PostMethod post = new PostMethod(config.solrBaseURL + "/" + archive + "/update");
    // PostMethod post = new PostMethod(config.solrBaseURL + config.solrNewIndex + "/update");
    post.setRequestEntity(new StringRequestEntity(xml, "text/xml", "utf-8"));
    post.setRequestHeader("Content-type", "text/xml; charset=utf-8");

    httpclient.getHttpConnectionManager().getParams().setConnectionTimeout(HTTP_CLIENT_TIMEOUT);
    httpclient.getHttpConnectionManager().getParams()
        .setIntParameter(HttpMethodParams.BUFFER_WARN_TRIGGER_LIMIT, 10000 * 1024);
    // Execute request
    try {
      int result;
      int solrRequestNumRetries = SOLR_REQUEST_NUM_RETRIES;
      do {
        result = httpclient.executeMethod(post);
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
        throw new IOException("Non-OK response: " + result + "\n\n" + xml);
      }
      String response = RDFIndexer.getResponseString(post);
      // log.info(response);
      Pattern pattern = Pattern.compile("status=\\\"(\\d*)\\\">(.*)\\<\\/result\\>", Pattern.DOTALL);
      Matcher matcher = pattern.matcher(response);
      while (matcher.find()) {
        String status = matcher.group(1);
        String message = matcher.group(2);
        if (!"0".equals(status)) {
          throw new IOException(message);
        }
      }
    } finally {
      // Release current connection to the connection pool once you are done
      post.releaseConnection();
    }
  }
  
  private Element convertObjectToSolrDOM(String documentName, HashMap<String, ArrayList<String>> fields) {

    Element doc = new Element("doc");
    for (Map.Entry<String, ArrayList<String>> entry: fields.entrySet()) {
      
      String field = entry.getKey();
      ArrayList<String> valList = entry.getValue();

      for (String value : valList) {
        Element f = new Element("field");
        f.setAttribute("name", field);
        ValidationUtility.populateTextField(f, value);
        doc.addContent(f);
      }
    }

    // tag the document with the batch id
    Element f = new Element("field");
    f.setAttribute("name", "batch");
    f.setText(guid);
    doc.addContent(f);

    return doc;
  }

  private class IndexerThread extends Thread {

    private boolean done;
    private HttpClient httpClient;

    public IndexerThread(int threadID) {
      setName("IndexerThread" + threadID);
      httpClient = new HttpClient();
      httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(HTTP_CLIENT_TIMEOUT);
      done = false;
    }

    @Override
    public void run() {
      File file = null;
      while ((file = dataFileQueue.poll()) != null) {
        indexFile(file, httpClient);
      }
      done = true;
    }

    public boolean isDone() {
      return done;
    }

  }


  /**
   * MAIN Main application entry point
   * @param args
   */
  public static void main(String[] args) {
    
    // Option constants
    final String fullTextFlag = "fulltext";   // This goes to the website to get the full text.
    final String reindexFlag = "reindex";     // This gets the full text from the current index instead of going to the website.
    final String maxDocsFlag = "maxDocs";     // This just looks at the first few docs in each folder.
    final String useIgnoreFile = "ignore";    // This ignores the folders specified in the file.
    final String useIncludeFile = "include";  // This ignores the folders specified in the file.
    final String compareAll = "compareFull";  // Ful compare of archive vs resources
    final String compareTxt = "compareTxt";   // compare ontly TEXT fields 
    final String compare = "compare";         // fast compare; everything BUT text
    final String source = "source";         // fast compare; everything BUT text
    final String archive = "archive";         // fast compare; everything BUT text
    
    // define the list of command line options
    Options options = new Options();
    Option srcOpt = new Option(source, true, "The source rdf file or directory to index");
    srcOpt.setRequired(true);
    options.addOption(srcOpt);
    Option nameOpt = new Option(archive, true, "The name of of the archive");
    nameOpt.setRequired(true);
    options.addOption(nameOpt);
    
    OptionGroup compareOpts = new OptionGroup();
    compareOpts.addOption( new Option(compareAll, false, "Compare the full content of each documet") );
    compareOpts.addOption( new Option(compareTxt, false, "Compare just text") );
    compareOpts.addOption( new Option(compare, false, "Compare everything but text") );
    options.addOptionGroup( compareOpts );
    
    OptionGroup textOpts = new OptionGroup();
    textOpts.addOption( new Option(fullTextFlag, false, "Retrieve full text from web") );
    textOpts.addOption( new Option(reindexFlag, false, "Retrieve full text from current index") );
    options.addOptionGroup( textOpts );   
    
    options.addOption(maxDocsFlag, true, "Max docs processed per folder");
    options.addOption(useIgnoreFile, true, "Ignore folders specified in this file");
    options.addOption(useIncludeFile, true, "Include folders specified in this file");
   
    // create parser and handle the options
    RDFIndexerConfig config = new RDFIndexerConfig();
    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse( options, args );
     
      // required params:
      config.rdfSource = new File( line.getOptionValue(source) );
      config.archiveName = line.getOptionValue(archive);
      
      // optional text handling flags
      if (line.hasOption(fullTextFlag)) {
        config.textMode = TextMode.RETRIEVE_FULL;
      } else if (line.hasOption(reindexFlag)) {
        config.textMode = TextMode.REINDEX_FULL;
      }
      
      // optional Compare flags
      if (line.hasOption(compareAll)) {
        config.compareMode = CompareMode.FULL;
      } else if (line.hasOption(compareTxt)) {
        config.compareMode = CompareMode.TEXT;
      } else if (line.hasOption(compare)) {
        config.compareMode = CompareMode.FAST;
      }
      
      // opt max docs per folder
      if ( line.hasOption(maxDocsFlag)) {
        config.maxDocsPerFolder = Integer.parseInt( line.getOptionValue(maxDocsFlag));
      }
      
      // opt files 
      config.ignoreFileName = line.getOptionValue(useIgnoreFile);
      config.includeFileName =  line.getOptionValue(useIncludeFile);
      
    } catch( ParseException exp ) {
      
      System.out.println("Error parsing options: " + exp.getMessage());
      
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "rdf-idexer", options );
      return;
    }

    // Launch the indexer with the parsed config
    config.populateFileLists();
    new RDFIndexer(config);
  }
}



