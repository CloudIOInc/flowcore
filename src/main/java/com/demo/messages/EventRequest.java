
package com.demo.messages;

import java.util.Map;

public class EventRequest<S extends Settings, C extends Context> {

  public enum TaskTypes {
    Input, Output, Transform
  }

  public String wfFlowId;
  String wfFlowInstanceId;
  String wfNodeId;
  String wfNodeInstanceId;
  String taskType;
  String fromTopic;
  String toTopic;
  Map<String, Integer> startOffset;
  Map<String, Integer> endOffset;
  String taskName;
  String startDate;
  Integer errorPartition;
  S settings;
  C context;

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

  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(String taskType) {
    this.taskType = taskType;
  }

  public String getFromTopic() {
    return fromTopic;
  }

  public void setFromTopic(String fromTopic) {
    this.fromTopic = fromTopic;
  }

  public String getToTopic() {
    return toTopic;
  }

  public void setToTopic(String toTopic) {
    this.toTopic = toTopic;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public String getStartDate() {
    return startDate;
  }

  public void setStartDate(String startDate) {
    this.startDate = startDate;
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

  public Integer getErrorPartition() {
    return errorPartition;
  }

  public void setErrorPartition(Integer errorPartition) {
    this.errorPartition = errorPartition;
  }

  public S getSettings() {
    return settings;
  }

  public void setSettings(S settings) {
    this.settings = settings;
  }

  public C getContext() {
    return context;
  }

  public void setContext(C context) {
    this.context = context;
  }

}
