
package io.cloudio.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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
import io.cloudio.producer.Producer;
import io.cloudio.util.GsonUtil;

public abstract class OutputTask<E extends TaskRequest<?>, D extends Data, O extends Data> extends BaseTask {
  protected E event;
  private String taskCode;
  protected String eventTopic;

  private DataConsumer dataConsumer;
  private TaskConsumer eventConsumer;
  private Producer producer;
  protected String groupId;

  private static Logger logger = LogManager.getLogger(OutputTask.class);

  OutputTask(String taskCode) {
    this.taskCode = taskCode;
    this.eventTopic = taskCode;
    this.groupId = taskCode + "-grId";
  }

  void onStart(TaskRequest<?> event) {

  }

  void onEnd(TaskRequest<?> event) {

  }

  public abstract void handleData(List<Data> data) throws Exception;

  public void start() {
    producer = Producer.get();
    eventConsumer = new TaskConsumer(groupId + "-" + UUID.randomUUID(), Collections.singleton(eventTopic));
    eventConsumer.createConsumer();
    eventConsumer.subscribe();
    subscribeEvent(eventTopic);
  }

  void subscribeEvent(String eventTopic) {
    Throwable ex = null;

    try {
      while (true) {
        ConsumerRecords<String, String> events = eventConsumer.poll();
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

            ex = commitAndHandleErrors(eventConsumer, partition, partitionRecords);
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

  private void handleEvent(TaskRequest<Settings> event) { // new event from wf engine
    unsubscribeEvent();
    this.event = (E) event;
    if (dataConsumer == null) {
      dataConsumer = new DataConsumer(groupId + "n3", Collections.singleton(event.getFromTopic()));
      dataConsumer.createConsumer();
      dataConsumer.subscribe();
      executorService.execute(() -> subscribeData(event.getFromTopic()));
    } else {
      dataConsumer.start();
    }

  }

  protected void post(List<O> data) throws Exception {
    // TODO: Check what if data size is large
    producer.beginTransaction();
    for (Data obj : data) {
      producer.send(event.getToTopic(), obj);
    }
    producer.commitTransaction();
  }

  protected void unsubscribeData() {
    dataConsumer.close();
    eventConsumer.start();
    // eventConsumer.wakeup();
    //subscribeEvent(eventTopic);
  }

  private void subscribeData(String fromTopic) {
    Throwable ex = null;
    while (true) {
      if (dataConsumer.canRun()) {
        try {
          ConsumerRecords<String, Data> dataRecords = dataConsumer.poll();
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
              if (list.size() > 0) {
                this.handleData(list);
              }
              ex = commitDataConsumer(ex, partition, partitionRecords, list);
            }
            if (ex != null) {
              throw ex;
            }
          }
        } catch (Throwable e) {
          logger.catching(e);
        }
      }
    }
  }

  private Throwable commitDataConsumer(Throwable ex, TopicPartition partition,
      List<ConsumerRecord<String, Data>> partitionRecords, List<Data> list) {
    try {
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

        dataConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
      } catch (Throwable e1) {
        dataConsumer.restartBeforeNextPoll();
      }
    }
    return ex;
  }

  private void unsubscribeEvent() {
    eventConsumer.close();
  }

}
