
package com.demo.messages;

import java.util.Date;
import java.util.Map;

public class EventResponse<C> {
  String wfFlowId;
  String wfFlowInstanceId;
  String wfNodeId;
  String wfNodeInstanceId;
  String status;
  String taskType;
  Map<String, Integer> startOffset;
  Map<String, Integer> endOffset;
  String statusMessage;
  String toTopic;
  Date startDate;
  Context context;

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

  public Map<String, Integer> getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(Map<String, Integer> startOffset) {
    this.startOffset = startOffset;
  }

  public Map<String, Integer> getEndOffset() {
    return endOffset;
  }

  public void setEndOffset(Map<String, Integer> endOffset) {
    this.endOffset = endOffset;
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public void setStatusMessage(String statusMessage) {
    this.statusMessage = statusMessage;
  }

  public String getToTopic() {
    return toTopic;
  }

  public void setToTopic(String toTopic) {
    this.toTopic = toTopic;
  }

  public Date getStartDate() {
    return startDate;
  }

  public void setStartDate(Date startDate) {
    this.startDate = startDate;
  }

  public Context getContext() {
    return context;
  }

  public void setContext(Context context) {
    this.context = context;
  }

  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(String taskType) {
    this.taskType = taskType;
  }

}
