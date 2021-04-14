
package io.cloudio.consumer;

import java.time.Duration;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.cloudio.task.Data;
import io.cloudio.util.JsonDeserializer;

public class Consumer extends BaseConsumer<String, Data> {

  // private static Logger logger = LogManager.getLogger(Consumer.class);

  public Consumer(String groupId, Collection<String> topicNames) {
    super(groupId, topicNames);
    // TODO Auto-generated constructor stub
  }

  @Override
  public KafkaConsumer<String, Data> createConsumer() {
    //override value.deserializer
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    //consumer = new KafkaConsumer<String, Data>(properties);

    consumer = new KafkaConsumer<String, Data>(properties, new StringDeserializer(),
        new JsonDeserializer<Data>(Data.class));
    return consumer;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  //CHANGED : Return type changed to ConsumerRecords so that task will commit if 
  //record handle is successful
  @Override
  public ConsumerRecords<String, Data> poll() throws Throwable {

    ConsumerRecords<String, Data> records = consumer.poll(Duration.ofSeconds(10));
    return records;

  }

}
