
package io.cloudio.messages;

import java.util.List;
import java.util.Map;

public class TaskEndResponse {
  private int executionId;
  private String endDate;
  private Map<String, String> outCome;
  private Map<String, Object> output;
  private String nodeUid;
  private String startDate;
  private String fromTopic;
  private List<Map<Integer, Integer>> fromTopicStartOffsets;
  private String toTopic;
  private String wfInstUid;
  private String wfUid;
  private Integer version;

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public String getAppUid() {
    return appUid;
  }

  public void setAppUid(String appUid) {
    this.appUid = appUid;
  }

  public String getOrgUid() {
    return orgUid;
  }

  public void setOrgUid(String orgUid) {
    this.orgUid = orgUid;
  }

  private String appUid;
  private String orgUid;

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

  public String getFromTopic() {
    return fromTopic;
  }

  public void setFromTopic(String fromTopic) {
    this.fromTopic = fromTopic;
  }

  public List<Map<Integer, Integer>> getFromTopicStartOffsets() {
    return fromTopicStartOffsets;
  }

  public void setFromTopicStartOffsets(List<Map<Integer, Integer>> fromTopicStartOffsets) {
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

  public String getEndDate() {
    return endDate;
  }

  public void setEndDate(String endDate) {
    this.endDate = endDate;
  }

  public Map<String, String> getOutCome() {
    return outCome;
  }

  public void setOutCome(Map<String, String> outCome) {
    this.outCome = outCome;
  }

  public Map<String, Object> getOutput() {
    return output;
  }

  public void setOutput(Map<String, Object> output) {
    this.output = output;
  }

}
