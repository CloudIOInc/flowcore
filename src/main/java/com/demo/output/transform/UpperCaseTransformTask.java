
package com.demo.output.transform;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.demo.events.TaskEventConsumer.TaskType;
import com.demo.messages.KafkaUtil;
import com.demo.messages.Record;
import com.demo.messages.Topics;

@Service
public class UpperCaseTransformTask {

  @Autowired
  private KafkaTemplate<String, Object> kafkaTemplate;

  UpperCaseTransformEventRequest message;

  @Value(value = "${topic.partition}")
  private int topicPartition;

  @Value(value = "${topic.replica}")
  private int topicReplica;

  @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
  private String bootstrapAddress;

  @Value(value = "${spring.kafka.consumer.group-id}")
  private String groupId;

  private Logger logger = Logger.getLogger("UpperCaseTransformTask");

  public void execute(UpperCaseTransformEventRequest message) {
    this.message = message;
    long stime = new Date().getTime();
    try {
      String topicName = Topics.UPPERCASE_TRANSFORM_TASK_TOPIC;
      KafkaUtil.createTopic(KafkaUtil.getAdminClient(), topicName, topicPartition, false);
      //create data topic
      String toTopic = message.getToTopic();
      KafkaUtil.createTopic(KafkaUtil.getAdminClient(), toTopic, topicPartition, false);
      Map<String, Integer> endOffsets = KafkaUtil.getOffsets(message.getFromTopic(), groupId, topicPartition, false);
      executeInternal(topicName, endOffsets);
      //  postEvent(message, totalRows);
      UpperCaseTranformTaskConsumer inputConsumer = new UpperCaseTranformTaskConsumer(message);
      KafkaUtil.executeOnKafkaScheduler(inputConsumer);
      postEvent(endOffsets);
    } catch (Exception e) {
      logger.severe("Failed to execute UpperCaseTranformTask" + e.getMessage());
    }
    long etime = new Date().getTime();
    logger.info("total time to load data in Kafka topic is " + (etime - stime) / 1000 + " seconds");
  }

  private void executeInternal(String topic, Map<String, Integer> endOffsets) {
    Set<String> set = endOffsets.keySet();
    Iterator<String> ite = set.iterator();
    while (ite.hasNext()) {
      String key = ite.next();
      Integer val = endOffsets.get(key);
      Record r = new Record();
      r.set("offset", val);
      r.set("topic", key);
      kafkaTemplate.send(topic, r);
    }
  }

  private void postEvent(Map<String, Integer> endOffsets) {
    UpperCaseTransormEventResponse response = new UpperCaseTransormEventResponse();
    response.setEndOffset(null);
    response.setStartOffset(null);
    response.setStatus("RUNNING");
    response.setTaskType(TaskType.Response.name());
    response.setStartDate(new Date());
    // response.setInputCount(totalRows);
    response.setWfFlowId(message.getWfFlowId());
    response.setWfFlowInstanceId(message.getWfFlowInstanceId());
    response.setStartOffset(endOffsets);
    kafkaTemplate.send(Topics.EVENT_TOPIC, response);
  }

}
