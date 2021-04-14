
package io.cloudio.task;

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
import io.cloudio.exceptions.CloudIOException;
import io.cloudio.messages.Settings;
import io.cloudio.producer.Producer;

public abstract class InputTask<E extends Event<? extends Settings>, O extends Data> {
  protected E event;
  private String taskCode;
  protected String eventTopic;

  private EventConsumer eventConsumer;
  private Producer producer;
  protected String groupId;

  private static Logger logger = LogManager.getLogger(Consumer.class);

  InputTask(String taskCode) {
    this.taskCode = taskCode;
    this.eventTopic = taskCode + "-events";
    this.groupId = taskCode + "-grId";
  }

  void onStart(Event<?> event) {

  }

  void onEnd(Event<?> event) {

  }

  public abstract void handleData(E event);

  public void start() {
    producer = Producer.get();
    eventConsumer = new EventConsumer(groupId, Collections.singleton(eventTopic));
    eventConsumer.createConsumer();
    eventConsumer.subscribe();
    subscribeEvent(eventTopic);
  }

  void subscribeEvent(String eventTopic) {
    Throwable ex = null;

    try {
      ConsumerRecords<String, String> events = eventConsumer.poll();
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
            Event<? extends Settings> eventObj = getEvent(eventSting);
            handleEvent((E) eventObj);
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

  protected abstract Event<? extends Settings> getEvent(String eventSting);

  private void handleEvent(E event) { // new event from wf engine
    unsubscribeEvent();
    this.event = event;
    handleData(this.event);
    startEvent();
  }

  protected void post(List<O> data) {
    try {
      producer.beginTransaction();
      for (Data obj : data) {
        producer.send(event.toTopic, obj);
      }
      producer.commitTransaction();
    } catch (Exception e) {
      logger.error("Error while posting data - " + e.getMessage());
      throw new CloudIOException(e);
    }
  }

  protected void startEvent() {
    eventConsumer.start();
    subscribeEvent(eventTopic);
  }

  private void unsubscribeEvent() {
    eventConsumer.close();
  }

}
