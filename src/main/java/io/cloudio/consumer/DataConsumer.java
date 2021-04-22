
package io.cloudio.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.cloudio.task.Data;
import io.cloudio.util.JsonDeserializer;

public class DataConsumer extends BaseConsumer<String, Data> {

  public DataConsumer(String groupId, Collection<String> topicNames) {
    super(groupId, topicNames);
  }

  @Override
  public KafkaConsumer<String, Data> createConsumer() {
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    consumer = new KafkaConsumer<String, Data>(properties, new StringDeserializer(),
        new JsonDeserializer<Data>(Data.class));
    return consumer;
  }

  @Override
  public String getName() {
    return topicNames.toString();
  }

  @Override
  public ConsumerRecords<String, Data> poll() throws Throwable {
    ConsumerRecords<String, Data> records = consumer.poll(Duration.ofSeconds(10));
    return records;

  }

  public Properties getProperties() {
    return properties;
  }

}
