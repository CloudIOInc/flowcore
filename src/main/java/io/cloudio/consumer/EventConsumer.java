
package io.cloudio.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.exceptions.CloudIOException;

public class EventConsumer extends BaseConsumer<String, String> {

  private static Logger logger = LogManager.getLogger(Consumer.class);

  public EventConsumer(String groupId, Collection<String> topicNames) {
    super(groupId, topicNames);
    // TODO Auto-generated constructor stub
  }

  @Override
  public KafkaConsumer<String, String> createConsumer() {
    //TODO: Update properties obj with deserilizer for events
	  
    //Properties copyProps = new Properties();
    //try {
      //BeanUtils.copyProperties(copyProps, properties);
    //} catch (Exception e) {
     // logger.error("Error while copying existing props " + e.getMessage());
     // throw new CloudIOException(e);
   // }
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    consumer = new KafkaConsumer<String, String>(properties);
    return consumer;
  }

  @Override
  public String getName() {
    return null;
  }

  //CHANGED : Return type changed to ConsumerRecords so that task will commit if 
  //record handle is successful
  @Override
  public ConsumerRecords<String, String> poll() throws Throwable {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
    return records;

  }

}