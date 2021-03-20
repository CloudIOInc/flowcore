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

import com.demo.util.Util;

public class OutputMessage extends Message {

  private OutputSettings outputSettings;
  private String dataTopic;

  public OutputMessage() {
  }

  public String getDataTopic() {
    return dataTopic;
  }

  public void setDataTopic(String targetTopic) {
    this.dataTopic = targetTopic;
  }

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

  public OutputSettings getOutputSettings() {
    return outputSettings;
  }

  public void setOutputSettings(OutputSettings outputSettings) {
    this.outputSettings = outputSettings;
  }

}
