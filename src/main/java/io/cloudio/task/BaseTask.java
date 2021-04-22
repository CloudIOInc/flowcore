
package io.cloudio.task;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.TaskConsumer;
import io.cloudio.messages.TaskEndResponse;
import io.cloudio.messages.TaskRequest;
import io.cloudio.messages.TaskStartResponse;
import io.cloudio.producer.Producer;
import io.cloudio.util.JsonUtils;
import io.cloudio.util.KafkaUtil;
import io.cloudio.util.ReaderUtil;

public class BaseTask {
  public static final String WF_EVENTS_TOPIC = "wf_events";
  private static Logger logger = LogManager.getLogger(BaseTask.class);
  private ConcurrentHashMap<String, List<HashMap<String, Object>>> schemaCache = new ConcurrentHashMap<>();
  ReaderUtil readerUtil = new ReaderUtil();

  protected void sendTaskEndResponse(TaskRequest taskRequest) throws Exception {
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

  protected void sendTaskStartResponse(TaskRequest taskRequest, String groupId) throws Exception {
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

  protected void seek(Collection<TopicPartition> partitions, List<Map<Integer, Integer>> fromTopicStartOffsets,
		  KafkaConsumer<String, Data> consumer) {
	  if(fromTopicStartOffsets != null) {
		 
		  Iterator<Map<Integer, Integer>> ite = fromTopicStartOffsets.listIterator();
		  Map<Integer,Integer> partionOffSetMap = new HashMap<Integer, Integer>();
	      while (ite.hasNext()) {
	        Map<Integer, Integer> map = ite.next();
	        partionOffSetMap.putAll(map);
	      }
	      for(TopicPartition partition: partitions) {
			  int partitionNumber = partition.partition();
    		  if(partionOffSetMap.containsKey(partitionNumber)) {
    			  long offset = partionOffSetMap.get(partitionNumber);
    			  if (logger.isWarnEnabled()) {
    		          logger.warn("seeking to offset  {} for partition {} for topic {}", 
    		        		   offset,partitionNumber,partition.topic());
    		        }
    			  consumer.seek(partition, offset);
    		  }
    	  }
		  
	  }
  }
  
  protected List<HashMap<String, Object>> getSchema(String tableName) throws Exception {
    if (schemaCache.get(tableName) != null) {
      return schemaCache.get(tableName);
    }
    return readerUtil.getSchema(tableName);
  }

  protected Properties getDBProperties() throws Exception {
    return readerUtil.getDBProperties();
  }

}
