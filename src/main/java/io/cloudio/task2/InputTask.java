
package io.cloudio.task2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.util.Util;

public abstract class InputTask extends BaseTask {

  protected AtomicBoolean isLeader = new AtomicBoolean(false);

  private static Logger logger = LogManager.getLogger(InputTask.class);

  InputTask(String taskCode) {
    super(taskCode);
  }

  public abstract void handleData(TaskRequest event) throws Exception;

  protected void createTopic(String eventTopic, int partitions) throws Exception {
    Util.createTopic(Util.getAdminClient(Util.getBootstrapServer()), eventTopic, partitions);
  }

  public void handleEvent() throws Throwable {
    unsubscribeEvent();
    try {
      handleData(taskRequest);
    } catch (Throwable t) {
      logger.catching(t);
      sendTaskEndResponse(taskRequest, true);
    }
    startEvent();
  }

  protected void sendTaskStartResponse(TaskRequest taskRequest, String groupId) throws Exception {

    TaskStartResponse response = new TaskStartResponse();
    response.setAppUid(taskRequest.getAppUid());
    response.setExecutionId(taskRequest.getExecutionId());
    response.setStartDate(taskRequest.getStartDate());
    response.setNodeUid(taskRequest.getNodeUid());
    response.setOrgUid(taskRequest.getOrgUid());
    List<Map<String, Integer>> offsets = Util.getOffsets(taskRequest.getToTopic(), groupId, Util.getBootstrapServer(),
        false);
    response.setFromTopicStartOffsets(offsets);
    response.setVersion(taskRequest.getVersion());
    response.setToTopic(taskRequest.getToTopic());
    response.setWfInstUid(taskRequest.getWfInstUid());
    response.setWfUid(taskRequest.getWfUid());

    try (Producer p = Producer.get()) {
      p.beginTransaction();
      p.send(WF_EVENTS_TOPIC, response);
      p.commitTransaction();
    }

    logger.info("Sending Input start response  for - {}-{} ", taskRequest.getNodeType(),
        taskRequest.getWfInstUid());
  }

}
