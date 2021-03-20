/*
 * Copyright (c) 2014 - present CloudIO Inc.
 * 1248 Reamwood Ave, Sunnyvale, CA 94089
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * CloudIO Inc. ("Confidential Information").  You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with CloudIO.
 */

package com.demo.messages;

public class ResponseMessage extends Message {

  private String dataTopic;
  private long inputCount;
  private long outputCount;

  public ResponseMessage() {
  }

  public String getDataTopic() {
    return dataTopic;
  }

  public void setDataTopic(String targetTopic) {
    this.dataTopic = targetTopic;
  }

  public long getInputCount() {
    return inputCount;
  }

  public void setInputCount(long inputCount) {
    this.inputCount = inputCount;
  }

  public long getOutputCount() {
    return outputCount;
  }

  public void setOutputCount(long outputCount) {
    this.outputCount = outputCount;
  }

}