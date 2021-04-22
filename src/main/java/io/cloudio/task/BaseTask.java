
package io.cloudio.task;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.TaskConsumer;
import io.cloudio.messages.TaskEndResponse;
import io.cloudio.messages.TaskRequest;
import io.cloudio.producer.Producer;
import io.cloudio.task.Data.EventType;
import io.cloudio.util.JsonUtils;
import io.cloudio.util.KafkaUtil;
import io.cloudio.util.ReaderUtil;

public class BaseTask {
  public static final String WF_EVENTS_TOPIC = "wf_events";
  private static Logger logger = LogManager.getLogger(BaseTask.class);
  private ConcurrentHashMap<String, List<HashMap<String, Object>>> schemaCache = new ConcurrentHashMap<>();
  ReaderUtil readerUtil = new ReaderUtil();
  Properties inputProps = null;
  static ExecutorService executorService = Executors.newFixedThreadPool(8);
  protected String bootStrapServer;
  protected int partitions;

  public BaseTask() {
    addShutdownHook();
  }

  /*
  {
    "appUid": "cloudio",
    "endDate": "2021-04-20T09:51:36.109358Z",
    "executionId": 1,
    "nodeUid": "oracle output",
    "orgUid": "cloudio",
    "outcome": {
      "status": "Success"
    },
    "output": {},
    "startDate": "2021-04-20T09:51:27.109358Z",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  */
  protected void sendTaskEndResponse(TaskRequest taskRequest, boolean isError) throws Exception {
    TaskEndResponse response = new TaskEndResponse();
    response.setAppUid(taskRequest.getAppUid());
    response.setEndDate(JsonUtils.dateToJsonString(new Date()));
    response.setExecutionId(taskRequest.getExecutionId());
    response.setNodeUid(taskRequest.getNodeUid());
    response.setOrgUid(taskRequest.getOrgUid());
    Map<String, String> outCome = new HashMap<String, String>();
    if (isError) {
      outCome.put("status", "Error");
    } else {
      outCome.put("status", "Success");
    }
    response.setOutCome(outCome);
    response.setOutput(new HashMap<String, Object>());
    response.setStartDate(taskRequest.getStartDate());
    response.setWfInstUid(taskRequest.getWfInstUid());
    response.setWfUid(taskRequest.getWfUid());
    response.setVersion(taskRequest.getVersion());

    try (Producer p = Producer.get()) {
      p.beginTransaction();
      p.send(WF_EVENTS_TOPIC, response);
      p.commitTransaction();
    }
    logger.info("Sending task end response  for - {}-{}-{} ", taskRequest.getNodeType(), taskRequest.getTaskType(),
        taskRequest.getWfInstUid());
  }

  protected Throwable commitAndHandleErrors(TaskConsumer consumer, TopicPartition partition,
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

  protected List<HashMap<String, Object>> getSchema(String tableName) throws Exception {
    List<HashMap<String, Object>> map = schemaCache.get(tableName);
    if (map == null) {
      map = readerUtil.getSchema(tableName);
      schemaCache.put(tableName, map);
    }
    return map;
  }

  protected Properties getDBProperties() throws Exception {
    if (inputProps == null) {
      inputProps = readerUtil.getDBProperties();
    }
    return inputProps;
  }

  public void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          logger.info("Closing resources!!");
          executorService.shutdown();
          executorService.awaitTermination(1, TimeUnit.MINUTES);
          Thread.sleep(1 * 20 * 1000);
        } catch (Exception e) {
          //ignore
        }
      }
    });
  }

  protected void sendEndMessage(TaskRequest taskRequest, String groupId) throws Exception {
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
