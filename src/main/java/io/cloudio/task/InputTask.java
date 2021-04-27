
package io.cloudio.task;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.util.Util;

public abstract class InputTask<K, V> extends BaseTask<K, V> {

  protected AtomicBoolean isLeader = new AtomicBoolean(false);

  private static Logger logger = LogManager.getLogger(InputTask.class);
  private Producer producer;

  InputTask(String taskCode) {
    super(taskCode);
  }

  public abstract Map<String, Object> executeTask(Map<String, Object> inputParams, Map<String, Object> outputParams,
      Map<String, Object> inputState) throws Exception;

  public void handleData(TaskRequest taskRequest) throws Exception {
    boolean isError = false;
    try {
      producer = Producer.get();
      producer.beginTransaction();
      if (!isSendStartResonse()) {
        sendTaskStartResponse(taskRequest, groupId);
        setSendStartResonse(true);
      }
      executeTask(taskRequest.getInputParams(), taskRequest.getOutputParams(),
          taskRequest.getInputState());
      producer.commitTransaction();
    } catch (Exception e) {
      logger.catching(e);
      producer.abortTransactionQuietly();
      isError = false;
      throw e;
    } finally {
      Util.closeQuietly(producer);
      sendTaskEndResponse(taskRequest, isError);
      if (!isError) {
        sendEndMessage();
      }
      setSendStartResonse(false);
    }
  }

  public void post(Data data) throws Exception {
    producer.send(taskRequest.getToTopic(), data);
  }

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
    response.setTaskType(taskRequest.getTaskType());

    try (Producer p = Producer.get()) {
      p.beginTransaction();
      p.send(WF_EVENTS_TOPIC, response);
      p.commitTransaction();
    }

    logger.info("Sending Input start response  for - {}-{} ", taskRequest.getNodeType(),
        taskRequest.getWfInstUid());
  }

  //  protected Data populateData(ResultSet rs, String tableName) throws Exception {
  //    Data d = new Data();
  //    List<HashMap<String, Object>> schema = getSchema(tableName);
  //    for (int i = 0; i < schema.size(); i++) {
  //      Map<String, Object> field = schema.get(i);
  //      String fieldName = (String) field.get("fieldName");
  //      Object obj = rs.getObject(fieldName);
  //      d.put(fieldName, obj);
  //    }
  //    return d;
  //  }
}
