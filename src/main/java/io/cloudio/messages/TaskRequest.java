
package io.cloudio.messages;

import java.util.List;
import java.util.Map;

public class TaskRequest<I, O> {

  public enum TaskType {
    Input, Output, Transform, Task
  }
  /*
  
  Transform:
  {
    "appUid": "cloudio",
    "executionId": 1,
    "inputState": {},
    "inputParams": {
      "sourceTableName": "DEPARTMENTS",
      "targetTableName": "DEPARTMENTS"
    },
    "outputParams": {},
    "nodeUid": "oracle transform",
    "nodeType": "OracleTransform",
    "orgUid": "cloudio",
    "settings": {},
    "startDate": "2021-04-20T09:55:19.889033Z",
    "taskType": "Transform",
    "fromTopic": "data_1",
    "fromTopicStartOffsets": [
      { "partition": 0, "offset": 122 },
      { "partition": 1, "offset": 100 }
    ],
    "toTopic": "data_2",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  
  Output
  {
    "appUid": "cloudio",
    "executionId": 1,
    "inputState": {},
    "inputParams": { "tableName": "DEPARTMENTS" },
    "outputParams": {},
    "nodeUid": "oracle output",
    "nodeType": "OracleOutput",
    "orgUid": "cloudio",
    "settings": {},
    "startDate": "2021-04-20T10:10:10.187088Z",
    "taskType": "Output",
    "fromTopic": "data_2",
    "fromTopicStartOffsets": [
      { "partition": 0, "offset": 234 },
      { "partition": 1, "offset": 333 }
    ],
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  
   * input
  {
    "appUid": "cloudio",
    "executionId": 1,
    "inputState": {},
    "inputParams": { "tableName": "DEPARTMENTS" },
    "outputParams": {},
    "nodeUid": "oracle input",
    "nodeType": "OracleInput",
    "orgUid": "cloudio",
    "settings": {},
    "startDate": "2021-04-20T09:51:06.109358Z",
    "taskType": "Input",
    "toTopic": "data_1",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  },
  
  "fromTopicStartOffsets": [
      { "partition": 0, "offset": 122 },
      { "partition": 1, "offset": 100 }
    ],
    */

  private int executionId;
  private I inputParams;
  private Map<String, Object> inputState;
  private O outputParams;
  private String nodeUid;
  private String nodeType;
  private String token;
  //token
  private Integer version;
  private String orgUid;
  private String startDate;
  private String taskType;
  private String fromTopic;
  private List<Map<String, Integer>> fromTopicStartOffsets;
  private String toTopic;
  private String wfInstUid;
  private String wfUid;
  private String appUid;

  public int getExecutionId() {
    return executionId;
  }

  public void setExecutionId(int executionId) {
    this.executionId = executionId;
  }

  public I getInputParams() {
    return inputParams;
  }

  public void setInputParams(I inputParams) {
    this.inputParams = inputParams;
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

  public Map<String, Object> getInputState() {
    return inputState;
  }

  public void setInputState(Map<String, Object> inputState) {
    this.inputState = inputState;
  }

  public O getOutputParams() {
    return outputParams;
  }

  public void setOutputParams(O outputParams) {
    this.outputParams = outputParams;
  }

  public String getNodeType() {
    return nodeType;
  }

  public void setNodeType(String nodeType) {
    this.nodeType = nodeType;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public String getOrgUid() {
    return orgUid;
  }

  public void setOrgUid(String orgUid) {
    this.orgUid = orgUid;
  }

  public String getAppUid() {
    return appUid;
  }

  public void setAppUid(String appUid) {
    this.appUid = appUid;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

}
