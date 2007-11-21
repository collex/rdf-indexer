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
    report = new FileWriter(reportFile);
    report.write("");
    fileCount = new HashSet<String>();
    objectCount = new HashSet<String>();
  }
  
  public void addError( Error e ) {
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

  public Summary getSummary() {
    return new Summary(fileCount.size(), objectCount.size(), errorCount);
  }

	public int getErrorCount() {
		return errorCount;
	}
}

class Error {
  private String filename,
      uri,
      errMsg;

  public Error(String filename, String uri, String errMsg) {
    this.filename = filename;
    this.uri = uri;
    this.errMsg = errMsg;
  }

  public String getFilename() {
    return filename;
  }

  public String getUri() {
    return uri;
  }

  public String toString() {
    return filename + "\t" + uri + "\t" + errMsg;
  }
}

class Message {
  private boolean failureFlag;
  private String errorMsg;

  public Message(boolean flag, String msg) {
    this.failureFlag = flag;
    this.errorMsg = msg;
  }

  public boolean getStatus() {
    return failureFlag;
  }

  public String getErrorMessage() {
    return errorMsg;
  }
}

class Summary {
  private int fileCount = 0,
      objectCount = 0,
      errorCount = 0;

  public Summary(int fileCount, int objectCount, int errorCount) {
    this.fileCount = fileCount;
    this.objectCount = objectCount;
    this.errorCount = errorCount;
  }

  public int getFileCount() {
    return fileCount;
  }

  public int getObjectCount() {
    return objectCount;
  }

  public int getErrorCount() {
    return errorCount;
  }
}
