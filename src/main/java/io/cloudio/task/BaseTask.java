
package io.cloudio.task;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.messages.TaskEndResponse;
import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.util.JsonUtils;
import io.cloudio.util.KafkaUtil;

public class BaseTask {
  public static final String WF_EVENTS_TOPIC = "WF_EVENTS";
  private static Logger logger = LogManager.getLogger(BaseTask.class);

  protected void sendTaskEndResponse(TaskRequest taskRequest) throws Exception {
    TaskEndResponse response = new TaskEndResponse();
    response.setExecutionId(taskRequest.getExecutionId());
    response.setNodeUid(taskRequest.getNodeUid());
    response.setWfInstUid(taskRequest.getWfInstUid());
    response.setWfUid(taskRequest.getWfUid());
    response.setTaskType(taskRequest.getTaskType());
    response.setStartDate(taskRequest.getStartDate());
    Map<String, String> outCome = new HashMap<String, String>();
    outCome.put("status", "Success");
    response.setOutCome(outCome);
    response.setEndDate(JsonUtils.dateToJsonString(new Date()));
    try (Producer p = Producer.get()) {
      p.beginTransaction();
      p.send(WF_EVENTS_TOPIC, response);
      p.commitTransaction();
    }
    logger.info("Sending task end response  - {}", response);
  }

  protected void sendTaskStartResponse(TaskRequest taskRequest, String groupId) throws Exception {
    TaskStartResponse response = new TaskStartResponse();
    response.setExecutionId(taskRequest.getExecutionId());
    response.setNodeUid(taskRequest.getNodeUid());
    response.setWfInstUid(taskRequest.getWfInstUid());
    response.setWfUid(taskRequest.getWfUid());
    response.setTaskType(taskRequest.getTaskType());
    response.setStartDate(taskRequest.getStartDate());
    List<Map<String, Integer>> offsets = KafkaUtil.getOffsets(taskRequest.getToTopic(), groupId, false);
    response.setFromTopicStartOffsets(offsets);
    try (Producer p = Producer.get()) {
      p.beginTransaction();
      p.send(WF_EVENTS_TOPIC, response);
      p.commitTransaction();
    }
    logger.info("Sending task start response  - {}", response);
  }

}
