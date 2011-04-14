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
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.log4j.Logger;

public class ErrorReport {
  public static Logger log = Logger.getLogger(ErrorReport.class.getName());
  private FileWriter report;

  private int errorCount;
  private Set<String> fileCount;
  private Set<String> objectCount;
  
  public ErrorReport( File reportFile ) throws IOException {
    //create the empty report.txt
    report = new FileWriter(reportFile, true);
    report.write("");
    fileCount = new HashSet<String>();
    objectCount = new HashSet<String>();
  }
  
  public void addError( IndexerError e ) {
    try {
		report.write(e.toString() + "\r\n");
	} 
    catch (IOException e1) {
		log.error("Unable to write error message to report file.");
	}
    fileCount.add(e.getFilename());
    objectCount.add(e.getUri());
    errorCount++;
  }
  
  public void flush() {
   try {
		report.flush();
	} catch (IOException e) {
		log.error("Error flushing report data to disk.");
	}
  }
  
  public void close() {
     try {
		report.flush();
	    report.close();	  
	} catch (IOException e) {
		log.error("Unable to properly close report file.");
	}
  }

  public ErrorSummary getSummary() {
    return new ErrorSummary(fileCount.size(), objectCount.size(), errorCount);
  }

	public int getErrorCount() {
		return errorCount;
	}
}