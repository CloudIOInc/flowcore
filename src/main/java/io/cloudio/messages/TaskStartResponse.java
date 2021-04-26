
package io.cloudio.messages;

import java.util.List;
import java.util.Map;

public class TaskStartResponse {

  private int executionId;
  private String nodeUid;
  private Integer version;
  private String appUid;
  private String orgUid;
  private String startDate;
  private String fromTopic; //omit null values
  private List<Map<String, Integer>> fromTopicStartOffsets;
  private String toTopic;
  private String wfInstUid;
  private String taskType;
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

  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(String taskType) {
    this.taskType = taskType;
  }

  /*
  
  // input
  {
  "appUid": "cloudio",
  "executionId": 1,
  "nodeUid": "oracle input",
  "orgUid": "cloudio",
  "startDate": "2021-04-20T09:51:06.109358Z",
  "taskType": "Input",
  "toTopic": "data_1",
  "version": 1,
  "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
  "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379",
  "fromTopicStartOffsets": [{ "partition": 0, "offset": 122 }, { "partition": 1, "offset": 100 }]
  }
  {
    "appUid": "cloudio",
    "endDate": "2021-04-20T09:51:16.109358Z",
    "executionId": 1,
    "nodeUid": "oracle input",
    "orgUid": "cloudio",
    "outcome": {
      "status": "Success"
    },
    "output": {},
    "startDate": "2021-04-20T09:51:06.109358Z",
    "taskType": "Input",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  
  // transform
  {
    "appUid": "cloudio",
    "executionId": 1,
    "fromTopic": "data_1",
    "fromTopicStartOffsets": [{ "partition": 0, "offset": 234 }, { "partition": 1, "offset": 333 }],
    "nodeUid": "oracle transform",
    "orgUid": "cloudio",
    "startDate": "2021-04-20T09:51:06.109358Z",
    "taskType": "Transform",
    "toTopic": "data_2",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  
  {
    "appUid": "cloudio",
    "endDate": "2021-04-20T09:51:26.109358Z",
    "executionId": 1,
    "nodeUid": "oracle transform",
    "orgUid": "cloudio",
    "outcome": {
      "status": "Success"
    },
    "output": {},
    "startDate": "2021-04-20T09:51:06.109358Z",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  
  // output
  {
    "appUid": "cloudio",
    "endDate": "2021-04-20T09:51:36.109358Z",
    "executionId": 1,
    "nodeUid": "oracle output",
    "orgUid": "cloudio",
    "outcome": {
      "status": "Success"
    },
    "output": {},
    "startDate": "2021-04-20T09:51:27.109358Z",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  */

}
