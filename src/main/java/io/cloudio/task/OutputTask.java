
package io.cloudio.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.DataConsumer;
import io.cloudio.messages.TaskRequest;

public abstract class OutputTask extends BaseTask {

  private DataConsumer dataConsumer;

  private static Logger logger = LogManager.getLogger(OutputTask.class);

  OutputTask(String taskCode) {
    super(taskCode + "-output");
  }

  public abstract void handleData(List<Data> data) throws Exception;

  public void handleEvent(TaskRequest event) {
    if (dataConsumer == null) {
      dataConsumer = new DataConsumer(groupId + "n3", Collections.singleton(event.getFromTopic()));
      dataConsumer.getProperties().put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
      dataConsumer.createConsumer();
      dataConsumer.subscribe();
      executorService.execute(() -> subscribeData(event.getFromTopic()));
    } else {
      dataConsumer.start();
    }

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
                try {
                  this.handleData(list);
                } catch (Throwable t) {
                  sendTaskEndResponse(taskRequest, true);
                }
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

  protected void unsubscribeData() {
    dataConsumer.close();
  }
}
