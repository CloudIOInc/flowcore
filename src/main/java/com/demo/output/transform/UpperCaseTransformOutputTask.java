
package com.demo.output.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demo.events.Producer;
import com.demo.messages.Record;
import com.demo.util.Util;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class UpperCaseTransformOutputTask {

  private Logger logger = LoggerFactory.getLogger(UpperCaseTransformOutputTask.class);

  private KafkaConsumer<String, String> kafkaConsumer;

  private UpperCaseTransformEventRequest message;

  private JsonArray schema;

  public UpperCaseTransformOutputTask(UpperCaseTransformEventRequest eventMessage, Properties consumerProperties,
      JsonArray schema) {

    kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    message = eventMessage;
    this.schema = schema;
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

      ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
      if (records.count() == 0) {
        break;
      }
      try (Producer producer = Producer.get()) {
        List<JsonObject> dataRecs = new ArrayList<JsonObject>();
        for (ConsumerRecord<String, String> record : records) {
          String message = record.value();
          logger.info("Received message: " + message);
          try {
            JsonElement data = JsonParser.parseString(message);
            dataRecs.add(data.getAsJsonObject());
          } catch (Exception e) {
            logger.error(e.getMessage());
          }

          {
            Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

            commitMessage.put(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));

            kafkaConsumer.commitSync(commitMessage);
          }
        }
        processData(producer, dataRecs);
        logger.info("Posted data successfully!");
      }
    }
  }

  private void initConsumer() {
    Map<String, Integer> offsets = message.getStartOffset();
    for (PartitionInfo partition : kafkaConsumer.partitionsFor(message.getToTopic())) {
      TopicPartition tp = new TopicPartition(message.getToTopic(), partition.partition());
      Integer offset = offsets.get(partition.partition());
      kafkaConsumer.seek(tp, offset);
    }
    // kafkaConsumer.assign(partitions);
  }

  public void processData(Producer producer, List<JsonObject> dataRecs) throws Exception {
    producer.beginTransaction();
    for (JsonObject obj : dataRecs) {
      Record r = Util.getRecord(obj);
      producer.send(message.getToTopic(), r);
    }
    producer.commitTransaction();
  }

}
