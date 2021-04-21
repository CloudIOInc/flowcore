
package io.cloudio.task;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.TaskConsumer;
import io.cloudio.exceptions.CloudIOException;
import io.cloudio.messages.Settings;
import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.task.Data.EventType;
import io.cloudio.util.KafkaUtil;

public abstract class InputTask<T extends TaskRequest<? extends Settings>, O extends Data> extends BaseTask {
  protected T taskRequest;
  protected String taskCode;
  protected String eventTopic;

  protected TaskConsumer taskConsumer;
  protected String groupId;

  protected String bootStrapServer;
  protected int partitions;

  protected AtomicBoolean isLeader = new AtomicBoolean(false);

  private static Logger logger = LogManager.getLogger(InputTask.class);

  InputTask(String taskCode) {
    this.taskCode = taskCode;
    this.eventTopic = taskCode;
    this.groupId = taskCode + "-grId";
  }

  void onStart(TaskRequest<?> event) {

  }

  void onEnd(TaskRequest<?> event) {

  }

  public abstract void handleData(T event) throws Exception;

  protected void createSubTaskConsumer() throws Exception {
  }

  protected void createTopics() throws Exception {
  }

  public void start(String bootStrapServer, int partitions) throws Exception {
    createTopic(eventTopic, bootStrapServer, partitions);
    this.bootStrapServer = bootStrapServer;
    this.partitions = partitions;
    taskConsumer = new TaskConsumer(groupId, Collections.singleton(eventTopic));
    taskConsumer.getProperties().put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    taskConsumer.createConsumer();
    taskConsumer.subscribe();
    createTopics();
    createSubTaskConsumer();
    subscribeEvent(eventTopic);
  }

  protected void createTopic(String eventTopic, String bootStrapServer, int partitions) throws Exception {
    KafkaUtil.createTopic(KafkaUtil.getAdminClient(bootStrapServer), eventTopic, partitions);
  }

  void subscribeEvent(String eventTopic) {
    Throwable ex = null;

    try {
      while (true) {
        if (taskConsumer.canRun()) {
          ConsumerRecords<String, String> events = taskConsumer.poll();
          // TODO : Can below code go in consumer ??
          if (events != null && events.count() > 0) {
            for (TopicPartition partition : events.partitions()) {
              List<ConsumerRecord<String, String>> partitionRecords = events.records(partition);
              if (partitionRecords.size() == 0) {
                continue;
              }
              if (logger.isInfoEnabled()) {
                logger.info("Got {} task events between {} & {} in {}", partitionRecords.size(),
                    partitionRecords.get(0).offset(),
                    partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
              }

              for (ConsumerRecord<String, String> record : partitionRecords) {
                String eventSting = record.value();
                if (eventSting == null) {
                  continue;
                }
                TaskRequest<? extends Settings> taskRequest = getTaskRequest(eventSting);
                isLeader.compareAndSet(false, true);
                handleEvent((T) taskRequest);
              }
              // isLeader.compareAndSet(true, false);
              ex = commitAndHandleErrors(taskConsumer, partition, partitionRecords);
            }
            if (ex != null) {
              throw ex;
            }
          }
        }
      }
    } catch (Throwable e) {
      logger.catching(e);
    }
    logger.debug("Stopped event consumer for {} task ", taskCode);
  }

  protected abstract T getTaskRequest(String eventSting);

  private void handleEvent(T event) throws Exception { // new event from wf engine
    unsubscribeEvent();
    this.taskRequest = event;
    handleData(this.taskRequest);
    startEvent();
  }

  protected void post(List<O> data, TaskRequest<Settings> taskRequest) {
    try {
      try (Producer producer = Producer.get()) {
        producer.beginTransaction();
        int i = 0;
        for (Data obj : data) {
          i++;
          producer.send(taskRequest.getToTopic(), "key-" + i, obj);
        }
        producer.commitTransaction();
      }
    } catch (Exception e) {
      logger.error("Error while posting data - " + e.getMessage());
      throw new CloudIOException(e);
    }
  }

  protected void startEvent() {
    taskConsumer.wakeup();
    taskConsumer.start();
    // subscribeEvent(eventTopic);
  }

  private void unsubscribeEvent() {
    taskConsumer.close();
  }

  protected void sendEndMessage() throws Exception {
    Data endMessage = new Data();
    endMessage.setEnd(EventType.End);
    List<Map<String, Integer>> offsets = KafkaUtil.getOffsets(taskRequest.getToTopic(), groupId, false);
    try (Producer p = Producer.get()) {
      p.beginTransaction();
      Iterator<Map<String, Integer>> ite = offsets.listIterator();
      while (ite.hasNext()) {
        Integer partition = ite.next().get("partition");
        p.send(taskRequest.getToTopic(), partition, null, endMessage);
      }
      p.commitTransaction();
      logger.info("Sending end data message for - {}", taskRequest.getToTopic());
    }
  }

  /*
  
  // input
  {
  "appUid": "cloudio",
  "executionId": 1,
  "nodeUid": "oracle input",
  "orgUid": "cloudio",
  "startDate": "2021-04-20T09:51:06.109358Z",
  "toTopic": "data_1",
  "version": 1,
  "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
  "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379",
  "fromTopicStartOffsets": [{ "partition": 0, "offset": 122 }, { "partition": 1, "offset": 100 }]
  }
  */
  protected void sendTaskStartResponse(TaskRequest taskRequest, String groupId) throws Exception {

    TaskStartResponse response = new TaskStartResponse();
    response.setAppUid(taskRequest.getAppUid());
    response.setExecutionId(taskRequest.getExecutionId());
    response.setStartDate(taskRequest.getStartDate());
    response.setNodeUid(taskRequest.getNodeUid());
    response.setOrgUid(taskRequest.getOrgUid());
    List<Map<String, Integer>> offsets = KafkaUtil.getOffsets(taskRequest.getToTopic(), groupId, false);
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
