
package io.cloudio.task;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.DataConsumer;
import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.util.Util;

public abstract class TransformTask<K, V> extends BaseTask<K, V> {

  private DataConsumer dataConsumer;
  private Producer producer;
  private static Logger logger = LogManager.getLogger(TransformTask.class);

  public TransformTask(String taskCode) {
    super(taskCode);
  }

  public void handleEvent() throws Exception {
    subscribeData(taskRequest.getFromTopic());
  }

  public abstract Map<String, Object> executeTask(Map<String, Object> inputParams, Map<String, Object> outputParams,
      Map<String, Object> inputState, Data data) throws Exception;

  protected void unsubscribeData() {
    dataConsumer.close();
  }

  private void subscribeData(String fromTopic) throws Exception {
    String _id = "dt_consumer_transform_" + taskRequest.getFromTopic();
    List<Map<String, Integer>> offsets = taskRequest.getFromTopicStartOffsets();
    Integer partition = offsets.get(0).get("partition");
    Integer offset = offsets.get(0).get("offset");
    TopicPartition part = new TopicPartition(fromTopic, partition);
    dataConsumer = new DataConsumer(_id,
        Collections.singleton(taskRequest.getFromTopic()), (BaseTask<String, Data>) this, part, offset);
    dataConsumer.run();
    dataConsumer.await();
    logger.info("Subscribing data event for transform task  - {}", _id);
  }

  public void handleData(List<Data> dataList) throws Exception {
    boolean isError = false;
    Data endMessage = null;
    try {
      int lastIndex = dataList.size() - 1;
      endMessage = dataList.get(lastIndex);
      if (endMessage.isEnd()) {
        unsubscribeData();
        dataConsumer.complete();
        dataList.remove(lastIndex);
      }
      if (!isSendStartResonse()) {
        logger.info("SendResponseFlag - {}", isSendStartResonse());
        sendTaskStartResponse(taskRequest, groupId);
        setSendStartResonse(true);
      }
      if (dataList.size() > 0) {
        producer = Producer.get();
        for (Data data : dataList) {
          producer.beginTransaction();
          executeTask(taskRequest.getInputParams(), taskRequest.getOutputParams(),
              taskRequest.getInputState(), data);
          producer.commitTransaction();
        }
      }

    } catch (Exception e) {
      producer.abortTransactionQuietly();
      logger.catching(e);
      dataConsumer.complete();
      isError = true;
      throw e;
    } finally {
      Util.closeQuietly(producer);
      if (endMessage != null && endMessage.isEnd()) {
        sendTaskEndResponse(taskRequest, isError);
        if (!isError) {
          sendEndMessage();
        }
      }

    }
  }

  protected void createTopic(String eventTopic, String bootStrapServer, int partitions) throws Exception {
    Util.createTopic(Util.getAdminClient(bootStrapServer), eventTopic, partitions);
  }

  public void post(Data data) throws Exception {
    producer.send(taskRequest.getToTopic(), data);
  }

  /*
  // transform
  {
    "appUid": "cloudio",
    "executionId": 1,
    "fromTopic": "data_1",
    "fromTopicStartOffsets": [{ "partition": 0, "offset": 234 }, { "partition": 1, "offset": 333 }],
    "nodeUid": "oracle transform",
    "orgUid": "cloudio",
    "startDate": "2021-04-20T09:51:06.109358Z",
    "toTopic": "data_2",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  */
  protected void sendTaskStartResponse(TaskRequest taskRequest, String groupId) throws Exception {

    TaskStartResponse response = new TaskStartResponse();
    response.setAppUid(taskRequest.getAppUid());
    response.setExecutionId(taskRequest.getExecutionId());
    response.setStartDate(taskRequest.getStartDate());
    response.setNodeUid(taskRequest.getNodeUid());
    response.setFromTopic(taskRequest.getFromTopic());
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

    logger.info("Sending Transform start response  for - {}-{} ", taskRequest.getNodeType(),
        taskRequest.getWfInstUid());
  }

}
