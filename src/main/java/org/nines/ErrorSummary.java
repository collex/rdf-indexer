/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.nines;

class ErrorSummary {
  private int fileCount = 0,
      objectCount = 0,
      errorCount = 0;

  public ErrorSummary(int fileCount, int objectCount, int errorCount) {
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
