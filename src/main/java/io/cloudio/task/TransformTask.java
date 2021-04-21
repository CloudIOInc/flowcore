
package io.cloudio.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.DataConsumer;
import io.cloudio.consumer.TaskConsumer;
import io.cloudio.messages.Settings;
import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.util.GsonUtil;
import io.cloudio.util.KafkaUtil;

public abstract class TransformTask<R extends TaskRequest<?>> extends BaseTask {

  public R taskRequest;
  private String taskCode;
  private String eventTopic;

  private DataConsumer dataConsumer;
  private TaskConsumer taskConsumer;
  private String eventConsumerGroupId;
  private String dataConsumerGroupId;

  private static Logger logger = LogManager.getLogger(TransformTask.class);

  public TransformTask(String taskCode) {

    this.taskCode = taskCode;
    this.eventTopic = taskCode;
    this.eventConsumerGroupId = taskCode + "-eventGrId";
    this.dataConsumerGroupId = taskCode + "-dataGrId";
  }

  public abstract List<Data> onData(TaskRequest<?> E, List<Data> D);

  void onStart(TaskRequest<?> request) {

  }

  void onEnd(TaskRequest<?> request) {

  }

  public void start(String bootStrapServer, int partitions) throws Exception {
    logger.info("TransformTask : start");
    createTopic(eventTopic, bootStrapServer, partitions);
    taskConsumer = new TaskConsumer(eventConsumerGroupId, Collections.singleton(eventTopic));
    taskConsumer.createConsumer(); // TODO : Revisit
    taskConsumer.subscribe();// TODO: Revisit
    subscribeEvent(eventTopic); // listen to workflow engine for instructions
    // subscribeData(); //TODO: events and data consumers run in parallel
  }

  void subscribeEvent(String eventTopic) {
    Throwable ex = null;

    try {
      while (taskConsumer.canRun()) {
        logger.info("eventConsumer poll() calling");
        ConsumerRecords<String, String> events = taskConsumer.poll();
        // if there is no event we should trigger subscribeEvent in loop
        // TODO : Can below code go in consumer ??
        if (events != null && events.count() > 0) {
          for (TopicPartition partition : events.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = events.records(partition);
            if (partitionRecords.size() == 0) {
              continue;
            }
            if (logger.isInfoEnabled()) {
              logger.info("EventConsumer : Got {} events between {} & {} in {}", partitionRecords.size(),
                  partitionRecords.get(0).offset(),
                  partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
            }

            for (ConsumerRecord<String, String> record : partitionRecords) {
              String eventSting = record.value();

              if (eventSting == null) {
                continue;
              }
              TaskRequest<Settings> eventObj = GsonUtil.getTransformTaskRequest(eventSting);
              handleEvent(eventObj);
            }

            try {

              if (partitionRecords.size() > 0) {
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                taskConsumer.commitSync(
                    Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
              }
            } catch (WakeupException | InterruptException e) {
              throw e;
            } catch (Throwable e) {
              logger.catching(e);
              if (ex == null) {
                ex = e;
              }
              try {
                logger.error("Seeking back {} events between {} & {} in {}", partitionRecords.size(),
                    partitionRecords.get(0).offset(),
                    partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());

                taskConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
              } catch (Throwable e1) {
                taskConsumer.restartBeforeNextPoll();
              }
            }
          }
          if (ex != null) {
            throw ex;
          }
        }
      }

    } catch (Throwable e) {
      logger.catching(e);
    }
    logger.debug("Stopped event consumer for {} task " + taskCode);
  }

  private void handleEvent(TaskRequest<Settings> request) throws Exception { // new event from wf engine
    //unsubscribeEvent();
    this.taskRequest = (R) request;
    if (dataConsumer != null) {
      dataConsumer.start();
    }
    sendTaskStartResponse(request, eventConsumerGroupId);
    subscribeData(taskRequest.getFromTopic());
  }

  protected void post(List<Data> data) throws Exception {
    // TODO: Check what if data size is large
    try (Producer producer = Producer.get()) {
      producer.beginTransaction();
      for (Data obj : data) {
        producer.send(taskRequest.getToTopic(), obj);
      }
      producer.commitTransaction();
    }
  }

  protected void unsubscribeData() {
    //closing the data consumer break the data consumer while loop and control goes to event consumer
    dataConsumer.close();

    //eventConsumer.start();
    //subscribeEvent(eventTopic);
  }

  private void subscribeData(String fromTopic) {
    Throwable ex = null;
    // TODO : handle while -> true
    if (dataConsumer == null) {
      dataConsumer = new DataConsumer(dataConsumerGroupId, Collections.singleton(fromTopic));
      dataConsumer.createConsumer();
      dataConsumer.subscribe();
    }
    while (dataConsumer.canRun()) {
      try {
        logger.debug("eventConsumer poll() calling");
        ConsumerRecords<String, Data> dataRecords = dataConsumer.poll();

        // TODO : Can below code go in consumer ??
        if (dataRecords.count() > 0) {
          for (TopicPartition partition : dataRecords.partitions()) {
            List<ConsumerRecord<String, Data>> partitionRecords = dataRecords.records(partition);
            if (partitionRecords.size() == 0) {
              continue;
            }
            if (logger.isInfoEnabled()) {
              logger.info("DataConsumer : Got {} events between {} & {} in {}", partitionRecords.size(),
                  partitionRecords.get(0).offset(),
                  partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
            }

            List<Data> list = new ArrayList<>(partitionRecords.size());
            for (ConsumerRecord<String, Data> record : partitionRecords) {
              Data data = record.value();
              list.add(data);
            }
            try {

              if (list.size() > 0) {
                this.handleData(list);
              }

              if (partitionRecords.size() > 0) {
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                dataConsumer.commitSync(
                    Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
              }
            } catch (WakeupException | InterruptException e) {
              throw e;
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

                taskConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
              } catch (Throwable e1) {
                taskConsumer.restartBeforeNextPoll();
              }
            }
          }
          if (ex != null) {
            throw ex;
          }
        }
      } catch (Throwable e) {
        logger.catching(e);
      }
    }
    logger.debug("Stopped data consumer for {} task " + taskCode);
  }

  public void handleData(List<Data> dataList) throws Exception {
    int lastIndex = dataList.size() - 1;
    Data lastMessage = dataList.get(lastIndex);
    if (lastMessage.isEnd()) {
      unsubscribeData();
      dataList.remove(lastIndex);
    }
    List<Data> output = this.onData(taskRequest, dataList);
    post(output);
  }

  private void unsubscribeEvent() {
    taskConsumer.close();
  }

  protected void createTopic(String eventTopic, String bootStrapServer, int partitions) throws Exception {
    KafkaUtil.createTopic(KafkaUtil.getAdminClient(bootStrapServer), eventTopic, partitions);
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
    List<Map<String, Integer>> offsets = KafkaUtil.getOffsets(taskRequest.getToTopic(), groupId, false);
    response.setFromTopicStartOffsets(offsets);
    try (Producer p = Producer.get()) {
      p.beginTransaction();
      p.send(WF_EVENTS_TOPIC, response);
      p.commitTransaction();
    }
    response.setVersion(taskRequest.getVersion());
    response.setToTopic(taskRequest.getToTopic());
    response.setWfInstUid(taskRequest.getWfInstUid());
    response.setWfUid(taskRequest.getWfUid());

    logger.info("Sending Transform end response  for - {}-{} ", taskRequest.getNodeType(),
        taskRequest.getWfInstUid());
  }

}
