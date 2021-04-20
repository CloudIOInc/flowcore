
package io.cloudio.task;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.EventConsumer;
import io.cloudio.exceptions.CloudIOException;
import io.cloudio.messages.Settings;
import io.cloudio.messages.TaskEndResponse;
import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.task.Data.EventType;
import io.cloudio.util.JsonUtils;
import io.cloudio.util.KafkaUtil;

public abstract class InputTask<T extends TaskRequest<? extends Settings>, O extends Data> {
  protected T taskRequest;
  protected String taskCode;
  protected String eventTopic;

  protected EventConsumer eventConsumer;
  protected String groupId;

  protected String bootStrapServer;
  protected int partitions;
  public static final String WF_EVENTS_TOPIC = "WF_EVENTS";

  protected AtomicBoolean isLeader = new AtomicBoolean(false);

  private static Logger logger = LogManager.getLogger(InputTask.class);

  InputTask(String taskCode) {
    this.taskCode = taskCode;
    this.eventTopic = taskCode + "-events";
    this.groupId = taskCode + "-grId";
  }

  void onStart(TaskRequest<?> event) {

  }

  void onEnd(TaskRequest<?> event) {

  }

  public abstract void handleData(T event) throws Exception;

  public void start(String bootStrapServer, int partitions) throws Exception {
    createTopic(eventTopic, bootStrapServer, partitions);
    this.bootStrapServer = bootStrapServer;
    this.partitions = partitions;

    eventConsumer = new EventConsumer(groupId, Collections.singleton(eventTopic));
    eventConsumer.getProperties().put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    eventConsumer.createConsumer();
    eventConsumer.subscribe();
    subscribeEvent(eventTopic);
  }

  protected void createTopic(String eventTopic, String bootStrapServer, int partitions) throws Exception {
    KafkaUtil.createTopic(KafkaUtil.getAdminClient(bootStrapServer), eventTopic, partitions);
  }

  void subscribeEvent(String eventTopic) {
    Throwable ex = null;

    try {
      while (true) {
        if (eventConsumer.canRun()) {
          ConsumerRecords<String, String> events = eventConsumer.poll();
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
              ex = commitAndHandleErrors(eventConsumer, partition, partitionRecords);
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

  protected Throwable commitAndHandleErrors(EventConsumer consumer, TopicPartition partition,
      List<ConsumerRecord<String, String>> partitionRecords) {
    Throwable ex = null;
    try {

      if (partitionRecords.size() > 0) {
        long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
        consumer.commitSync(
            Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
        logger.info("commiting offset - {}, partition - {}", lastOffset, partition);
      }
    } catch (WakeupException | InterruptException e) {
      //throw e;
    } catch (Throwable e) {
      logger.catching(e);
      if (ex == null) {
        ex = e;
      }
      try {
        logger.error("Seeking back {} events between {} & {} in {}", partitionRecords.size(),
            partitionRecords.get(0).offset(),
            partitionRecords.get(partitionRecords.size() - 1).offset(),
            partition.toString());

        consumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
      } catch (Throwable e1) {
        logger.error(e1);
        consumer.restartBeforeNextPoll();
      }
    }
    return ex;
  }

  protected abstract T getTaskRequest(String eventSting);

  private void handleEvent(T event) throws Exception { // new event from wf engine
    unsubscribeEvent();
    this.taskRequest = event;
    handleData(this.taskRequest);
    startEvent();
  }

  protected void post(List<O> data) {
    try {
      try (Producer producer = Producer.get()) {
        producer.beginTransaction();
        for (Data obj : data) {
          producer.send(taskRequest.getToTopic(), obj);
        }
        producer.commitTransaction();
      }
    } catch (Exception e) {
      logger.error("Error while posting data - " + e.getMessage());
      throw new CloudIOException(e);
    }
  }

  protected void startEvent() {
    eventConsumer.wakeup();
    eventConsumer.start();
    // subscribeEvent(eventTopic);
  }

  private void unsubscribeEvent() {
    eventConsumer.close();
  }

  protected void sendTaskEndResponse() throws Exception {
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

  protected void sendTaskStartResponse() throws Exception {
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

}
