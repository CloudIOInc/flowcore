
package io.cloudio.task;

import java.util.ArrayList;
import java.util.Collections;
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

import io.cloudio.consumer.DataConsumer;
import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.util.Util;

public abstract class TransformTask extends BaseTask {

  private DataConsumer dataConsumer;
  private String dataConsumerGroupId;
  private String bootStrapServer;
  private Producer producer;
  private AtomicBoolean consumeData = new AtomicBoolean(false);
  private static Logger logger = LogManager.getLogger(TransformTask.class);

  public TransformTask(String taskCode) {
    super(taskCode + "-transform");
  }

  public void handleEvent(TaskRequest request) throws Exception { // new event from wf engine
    unsubscribeEvent();
    sendTaskStartResponse(request, groupId);
    subscribeData(taskRequest.getFromTopic());
  }

  public abstract void executeTask(Map<String, Object> inputParams, Map<String, Object> outputParams,
      Map<String, Object> inputState, List<Data> dataList);

  protected void unsubscribeData() {
    dataConsumer.close();
  }

  private void subscribeData(String fromTopic) {
    Throwable ex = null;
    if (dataConsumer == null) {
      dataConsumer = new DataConsumer(dataConsumerGroupId, Collections.singleton(fromTopic));
      dataConsumer.getProperties().put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
      dataConsumer.createConsumer();
      dataConsumer.subscribe();
    }
    while (true) {
      try {
        if (dataConsumer.canRun()) {
          logger.debug("eventConsumer poll() calling");
          ConsumerRecords<String, Data> dataRecords = dataConsumer.poll();
          if (dataRecords.count() > 0) {
            if (!consumeData.get()) {
              consumeData.compareAndSet(false, true);
            }
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
        } else {
          Thread.sleep(10 * 10000); //sleep for 10 seconds
        }
      } catch (Throwable e) {
        logger.catching(e);
      }

    }
  }

  public void handleData(List<Data> dataList) throws Exception {
    boolean isError = false;
    Data endMessage = null;
    try {
      int lastIndex = dataList.size() - 1;
      endMessage = dataList.get(lastIndex);
      if (endMessage.isEnd()) {
        unsubscribeData();
        dataList.remove(lastIndex);
      }
      if (dataList.size() > 0) {
        producer = Producer.get();
        producer.beginTransaction();
        executeTask(taskRequest.getInputParams(), taskRequest.getOutputParams(),
            taskRequest.getInputState(), dataList);
        producer.commitTransaction();
      }

    } catch (Exception e) {
      producer.abortTransactionQuietly();
      logger.catching(e);
      throw e;
    } finally {
      Util.closeQuietly(producer);
      if (endMessage != null) {
        sendTaskEndResponse(taskRequest, isError);
      }
    }
  }

  protected void createTopic(String eventTopic, String bootStrapServer, int partitions) throws Exception {
    Util.createTopic(Util.getAdminClient(bootStrapServer), eventTopic, partitions);
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
    List<Map<String, Integer>> offsets = Util.getOffsets(taskRequest.getToTopic(), groupId, false);
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
