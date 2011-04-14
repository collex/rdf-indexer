/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.nines;


class ErrorMessage {
  private boolean failureFlag;
  private String errorMsg;

  public ErrorMessage(boolean flag, String msg) {
    this.failureFlag = flag;
    this.errorMsg = msg;
  }

  public boolean getStatus() {
    return failureFlag;
  }

  public String getErrorMessage() {
    return errorMsg;
  }
  
  @Override
  public String toString() {
    return getErrorMessage();
  }
}