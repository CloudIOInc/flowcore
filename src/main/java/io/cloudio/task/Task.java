
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

public abstract class Task<E extends TaskRequest<?>, D extends Data, O extends Data> {
  protected E event;
  private String taskCode;
  protected String eventTopic;

  // TODO: FIXME : EventConsumer and Consumer should run in thread ??
  private Consumer dataConsumer;
  private EventConsumer eventConsumer;
  private Producer producer;
  protected String groupId;

  private static Logger logger = LogManager.getLogger(Task.class);

  Task(String taskCode) {
    this.taskCode = taskCode;
    this.eventTopic = taskCode + "-events";
    this.groupId = taskCode + "-grId";
  }

  void onStart(TaskRequest<?> event) {

  }

  void onEnd(TaskRequest<?> event) {

  }

  public abstract void handleData(List<Data> data);

  public void start() {

    producer = Producer.get();

    eventConsumer = new EventConsumer(groupId, Collections.singleton(eventTopic));
    eventConsumer.createConsumer(); // TODO : Revisit
    eventConsumer.subscribe();// TODO: Revisit
    subscribeEvent(eventTopic); // listen to workflow engine for instructions
    //subscribeData(); //TODO: events and data consumers run in parallel
  }

  void subscribeEvent(String eventTopic) {
    Throwable ex = null;

    try {
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
            logger.info("Got {} events between {} & {} in {}", partitionRecords.size(),
                partitionRecords.get(0).offset(),
                partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
          }

          for (ConsumerRecord<String, String> record : partitionRecords) {
            String eventSting = record.value();
            if (eventSting == null) {
              continue;
            }
            TaskRequest<Settings> eventObj = GsonUtil.getEventObject(eventSting);
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
    logger.debug("Stopped event consumer for {} task " + taskCode);
  }

  private void handleEvent(TaskRequest<Settings> event) { // new event from wf engine
    unsubscribeEvent();
    this.event = (E) event;
    subscribeData(event.getFromTopic());
  }

  protected void post(List<O> data) {
    try {
      // TODO: Check what if data size is large
      producer.beginTransaction();
      for (Data obj : data) {
        producer.send(event.getToTopic(), obj);
      }
      producer.commitTransaction();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected void unsubscribeData() {
    dataConsumer.close();
    eventConsumer.start();
    subscribeEvent(eventTopic);
  }

  private void subscribeData(String fromTopic) {
    Throwable ex = null;
    // TODO : handle while -> true
    if (dataConsumer == null) {
      dataConsumer = new Consumer(groupId + "n3", Collections.singleton(fromTopic));
      dataConsumer.createConsumer();
      dataConsumer.subscribe();
    }
    while (dataConsumer.canRun()) {
      try {
        ConsumerRecords<String, Data> dataRecords = dataConsumer.poll();
        // TODO : Can below code go in consumer ??
        if (dataRecords.count() > 0) {
          for (TopicPartition partition : dataRecords.partitions()) {
            List<ConsumerRecord<String, Data>> partitionRecords = dataRecords.records(partition);
            if (partitionRecords.size() == 0) {
              continue;
            }
            if (logger.isInfoEnabled()) {
              logger.info("Got {} events between {} & {} in {}", partitionRecords.size(),
                  partitionRecords.get(0).offset(),
                  partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
            }

            List<Data> list = new ArrayList<>(partitionRecords.size());
            for (ConsumerRecord<String, Data> record : partitionRecords) {
              Data data = record.value();
              if (data == null) {
                continue;
              }
              list.add(data);
            }
            try {

              if (list.size() > 0) {
                this.handleData(list);
              }

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

  private void unsubscribeEvent() {
    eventConsumer.close();
  }

}
