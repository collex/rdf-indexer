/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.nines;

class IndexerError {
  private String filename,
      uri,
      errMsg;

  public IndexerError(String filename, String uri, String errMsg) {
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