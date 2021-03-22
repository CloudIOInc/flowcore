
package com.demo.output.transform;

import java.time.Duration;
import java.util.ArrayList;
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

import com.demo.events.BaseConsumer;
import com.demo.events.Producer;
import com.demo.messages.Record;
import com.google.gson.JsonArray;

public class UpperCaseTransformOutputTask {

  private Logger logger = LoggerFactory.getLogger(UpperCaseTransformOutputTask.class);

  private KafkaConsumer<String, Record> kafkaConsumer;

  private UpperCaseTransformEventRequest message;

  private JsonArray schema;

  public UpperCaseTransformOutputTask(String consumerGroupId, UpperCaseTransformEventRequest eventMessage) {

    kafkaConsumer = new KafkaConsumer<>(BaseConsumer.getProperties(consumerGroupId));
    message = eventMessage;
    //this.schema = schema;
  }

  /**
   * This function will start a single worker thread per topic.
   * After creating the consumer object, we subscribed to a list of Kafka topics, in the constructor.
   * For this example, the list consists of only one topic. But you can give it a try with multiple topics.
   * @throws Exception 
   */
  public void execute() throws Exception {

    /*
     * We will start an infinite while loop, inside which we'll be listening to
     * new messages in each topic that we've subscribed to.
     */

    //kafkaConsumer.poll(0);
    initConsumer();

    while (true) {

      ConsumerRecords<String, Record> records = kafkaConsumer.poll(Duration.ofSeconds(60));
      if (records.count() == 0) {
        break;
      }
      try (Producer producer = Producer.get()) {
        List<Record> dataRecs = new ArrayList<Record>();
        ConsumerRecord<String, String> lastRecord = null;
        for (ConsumerRecord<String, Record> record : records) {
          Record message = record.value();
          logger.info("Received message: " + message);
          try {
            dataRecs.add(message);

            Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

            commitMessage.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()),
                new OffsetAndMetadata(lastRecord.offset() + 1));

            kafkaConsumer.commitSync(commitMessage);
          } catch (Exception e) {
            logger.error(e.getMessage());
          }
        }

        processData(producer, dataRecs);
        logger.info("Posted data successfully!");
      }
    }
  }

  private void initConsumer() {
    Map<String, Integer> offsets = message.getStartOffset();
    for (PartitionInfo partition : kafkaConsumer.partitionsFor(message.getFromTopic())) {
      TopicPartition tp = new TopicPartition(message.getFromTopic(), partition.partition());
      String _tp = tp.toString();
      Integer offset = offsets.get(_tp);
      List<TopicPartition> _tps = new ArrayList<TopicPartition>();
      _tps.add(tp);
      kafkaConsumer.assign(_tps);
      kafkaConsumer.seek(tp, offset);
    }
    // kafkaConsumer.assign(partitions);
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

}
