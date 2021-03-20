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

public class TaskEvent extends Message {

  private InputSettings inputSettings;
  private String dataTopic;

  public TaskEvent() {
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

  public InputSettings getInputSettings() {
    return inputSettings;
  }

  public void setInputSettings(InputSettings inputSettings) {
    this.inputSettings = inputSettings;
  }

}
