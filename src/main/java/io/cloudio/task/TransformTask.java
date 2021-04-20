
package io.cloudio.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.Consumer;
import io.cloudio.consumer.EventConsumer;
import io.cloudio.messages.Settings;
import io.cloudio.messages.TaskRequest;
import io.cloudio.producer.Producer;
import io.cloudio.util.GsonUtil;
import io.cloudio.util.KafkaUtil;

public abstract class TransformTask<R extends TaskRequest<?>> {

  public R taskRequest;
  private String taskCode;
  private String eventTopic;

  // TODO: FIXME : EventConsumer and Consumer should run in thread ??
  private Consumer dataConsumer;
  private EventConsumer eventConsumer;
  private Producer producer;
  private String eventConsumerGroupId;
  private String dataConsumerGroupId;

  private static Logger logger = LogManager.getLogger(TransformTask.class);

  public TransformTask(String taskCode) {

    this.taskCode = taskCode;
    this.eventTopic = taskCode + "-events";
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
    producer = Producer.get();
    createTopic(eventTopic, bootStrapServer, partitions);
    eventConsumer = new EventConsumer(eventConsumerGroupId, Collections.singleton(eventTopic));
    eventConsumer.createConsumer(); // TODO : Revisit
    eventConsumer.subscribe();// TODO: Revisit
    subscribeEvent(eventTopic); // listen to workflow engine for instructions
    // subscribeData(); //TODO: events and data consumers run in parallel
  }

  void subscribeEvent(String eventTopic) {
    Throwable ex = null;

    try {
      while (eventConsumer.canRun()) {
        logger.info("eventConsumer poll() calling");
        ConsumerRecords<String, String> events = eventConsumer.poll();
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
                eventConsumer.commitSync(
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

                eventConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
              } catch (Throwable e1) {
                eventConsumer.restartBeforeNextPoll();
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

  private void handleEvent(TaskRequest<Settings> request) { // new event from wf engine
    //unsubscribeEvent();
    this.taskRequest = (R) request;
    if (dataConsumer != null) {
      dataConsumer.start();
    }
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
      dataConsumer = new Consumer(dataConsumerGroupId, Collections.singleton(fromTopic));
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

                eventConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
              } catch (Throwable e1) {
                eventConsumer.restartBeforeNextPoll();
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
    eventConsumer.close();
  }

  protected void createTopic(String eventTopic, String bootStrapServer, int partitions) throws Exception {
    KafkaUtil.createTopic(KafkaUtil.getAdminClient(bootStrapServer), eventTopic, partitions);
  }

}
