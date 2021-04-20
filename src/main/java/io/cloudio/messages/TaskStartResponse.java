
package io.cloudio.messages;

import java.util.List;
import java.util.Map;

public class TaskStartResponse {

  private int executionId;
  private String nodeUid;
  private String startDate;
  private String taskType;
  private String fromTopic;
  private List<Map<String, Integer>> fromTopicStartOffsets;
  private String toTopic;
  private String wfInstUid;
  private String wfUid;

  public int getExecutionId() {
    return executionId;
  }

  public void setExecutionId(int executionId) {
    this.executionId = executionId;
  }

  public String getNodeUid() {
    return nodeUid;
  }

  public void setNodeUid(String nodeUid) {
    this.nodeUid = nodeUid;
  }

  public String getStartDate() {
    return startDate;
  }

  public void setStartDate(String startDate) {
    this.startDate = startDate;
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

  public List<Map<String, Integer>> getFromTopicStartOffsets() {
    return fromTopicStartOffsets;
  }

  public void setFromTopicStartOffsets(List<Map<String, Integer>> fromTopicStartOffsets) {
    this.fromTopicStartOffsets = fromTopicStartOffsets;
  }

  public String getToTopic() {
    return toTopic;
  }

  public void setToTopic(String toTopic) {
    this.toTopic = toTopic;
  }

  public String getWfInstUid() {
    return wfInstUid;
  }

  public void setWfInstUid(String wfInstUid) {
    this.wfInstUid = wfInstUid;
  }

  public String getWfUid() {
    return wfUid;
  }

  public void setWfUid(String wfUid) {
    this.wfUid = wfUid;
  }

}
