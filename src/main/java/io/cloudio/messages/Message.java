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

package io.cloudio.messages;

import io.cloudio.util.Util;

public abstract class Message {

  private MessageKey key = null;
  private long offset;
  private String eventType;
  private String wfFlowId;
  private String wfFlowInstanceId;
  private String wfNodeId;
  private String wfNodeInstanceId;
  private String status;
  private String targetTopic;

  public Message() {
  }

  public MessageKey getKey() {
    return key;
  }

  public String getKeyString() {
    return key == null ? null : key.toString();
  }

  public long offset() {
    return offset;
  }

  public void setKey(MessageKey key) {
    this.key = key;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getWfFlowId() {
    return wfFlowId;
  }

  public void setWfFlowId(String wfFlowId) {
    this.wfFlowId = wfFlowId;
  }

  public String getWfFlowInstanceId() {
    return wfFlowInstanceId;
  }

  public void setWfFlowInstanceId(String wfFlowInstanceId) {
    this.wfFlowInstanceId = wfFlowInstanceId;
  }

  public String getWfNodeId() {
    return wfNodeId;
  }

  public void setWfNodeId(String wfNodeId) {
    this.wfNodeId = wfNodeId;
  }

  public String getWfNodeInstanceId() {
    return wfNodeInstanceId;
  }

  public void setWfNodeInstanceId(String wfNodeInstanceId) {
    this.wfNodeInstanceId = wfNodeInstanceId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getTargetTopic() {
    return targetTopic;
  }

  public void setTargetTopic(String targetTopic) {
    this.targetTopic = targetTopic;
  }

}
