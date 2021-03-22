
package com.demo.output.transform;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.demo.events.BaseConsumer;
import com.demo.events.Producer;
import com.demo.events.TaskEventConsumer.TaskType;
import com.demo.messages.EventRequest;
import com.demo.messages.KafkaUtil;
import com.demo.messages.Record;
import com.demo.messages.Topics;
import com.demo.output.OutputTask;

public class UpperCaseTransformOutputTask extends OutputTask {

  private KafkaConsumer<String, Record> kafkaConsumer;

  private UpperCaseTransformEventRequest message;

  private int topicPartition;

  KafkaTemplate<String, Object> kafkaTemplate;

  private String groupId;
  private Logger logger = LoggerFactory.getLogger(UpperCaseTransformOutputTask.class);

  public UpperCaseTransformOutputTask(String groupId, UpperCaseTransformEventRequest eventMessage,
      int topicPartition, KafkaTemplate<String, Object> kafkaTemplate) throws Exception {

    kafkaConsumer = new KafkaConsumer<>(BaseConsumer.getProperties(Topics.UPPERCASE_GROUP_ID));
    message = eventMessage;
    this.groupId = groupId;
    this.topicPartition = topicPartition;
    this.kafkaTemplate = kafkaTemplate;
    KafkaUtil.createTopic(KafkaUtil.getAdminClient(), eventMessage.getToTopic(), 1, false);

  }

  private void postEvent(Map<String, Integer> startOffsets) {
    UpperCaseTransormEventResponse response = new UpperCaseTransormEventResponse();
    response.setEndOffset(null);
    response.setStartOffset(null);
    response.setStatus("COMPLETE");
    response.setTaskType(TaskType.Response.name());
    response.setStartDate(new Date());
    // response.setInputCount(totalRows);
    response.setWfFlowId(message.getWfFlowId());
    response.setWfFlowInstanceId(message.getWfFlowInstanceId());
    Map<String, Integer> endOffsets = KafkaUtil.getOffsets(message.getToTopic(), groupId, topicPartition, false);
    response.setStartOffset(startOffsets);
    response.setEndOffset(endOffsets);
    kafkaTemplate.send(Topics.EVENT_TOPIC, response);
  }

  /**
   * This function will start a single worker thread per topic.
   * After creating the consumer object, we subscribed to a list of Kafka topics, in the constructor.
   * For this example, the list consists of only one topic. But you can give it a try with multiple topics.
   * @throws Exception 
   */
  public void execute() throws Exception {
    initConsumer(kafkaConsumer, message);
    Map<String, Integer> startOffsets = KafkaUtil.getOffsets(message.getToTopic(), groupId, topicPartition, true);
    while (true) {
      ConsumerRecords<String, Record> records = kafkaConsumer.poll(Duration.ofMillis(10000));
      if (records.count() == 0) {
        postEvent(startOffsets);
        break;
      }
      try (Producer producer = Producer.get()) {
        List<Record> dataRecs = new ArrayList<Record>();
        ConsumerRecord<String, Record> lastRecord = null;
        for (ConsumerRecord<String, Record> record : records) {
          Record message = record.value();
          logger.info("Received message: " + message);
          try {
            dataRecs.add(message);
            lastRecord = record;
          } catch (Exception e) {
            logger.error(e.getMessage());
          }

        }
        Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

        commitMessage.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()),
            new OffsetAndMetadata(lastRecord.offset() + 1));

        kafkaConsumer.commitSync(commitMessage);

        processData(producer, dataRecs);
        logger.info("Posted data successfully!");
      }
    }
  }

  public void processData(Producer producer, List<Record> dataRecs) throws Exception {
    producer.beginTransaction();
    List<String> fields = message.getSettings().getTransforFields();
    boolean isTransorm = fields != null && fields.size() > 0;
    for (Record obj : dataRecs) {
      if (isTransorm) {
        transform(fields, obj);
      }
      producer.send(message.getToTopic(), obj);
    }
    producer.commitTransaction();
  }

  private void transform(List<String> fields, Record r) {
    fields.forEach(field -> {
      String str = r.getAsString(field);
      if (str != null) {
        r.set(field, str.toUpperCase());
      }
    });

  }

  public void initConsumer(KafkaConsumer<String, Record> kafkaConsumer, EventRequest<?, ?> message) {
    Map<String, Integer> offsets = message.getStartOffset();
    for (PartitionInfo partition : kafkaConsumer.partitionsFor(message.getFromTopic())) {
      TopicPartition tp = new TopicPartition(message.getFromTopic(), partition.partition());
      String _tp = tp.toString();
      Integer offset = offsets.get(_tp);
      List<TopicPartition> _tps = new ArrayList<TopicPartition>();
      _tps.add(tp);
      kafkaConsumer.assign(_tps);
      // kafkaConsumer.seek(tp, offset);
    }
    // kafkaConsumer.assign(partitions);
  }

}
