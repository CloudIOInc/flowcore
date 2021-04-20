
package io.cloudio.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TaskConsumer extends BaseConsumer<String, String> {

  public TaskConsumer(String groupId, Collection<String> topicNames) {
    super(groupId, topicNames);
  }

  @Override
  public KafkaConsumer<String, String> createConsumer() {
    consumer = new KafkaConsumer<String, String>(properties);
    return consumer;
  }

  @Override
  public String getName() {
    return topicNames.toString();
  }

  //CHANGED : Return type changed to ConsumerRecords so that task will commit if 
  //record handle is successful
  @Override
  public ConsumerRecords<String, String> poll() throws Throwable {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
    return records;

  }

  public Properties getProperties() {
    return properties;
  }

}