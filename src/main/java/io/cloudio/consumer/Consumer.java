
package io.cloudio.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.cloudio.util.CloudIOException;

public class Consumer<K, V> extends BaseConsumer<K, V> {

  public Consumer(String groupId, Collection<String> topicNames) {
    super(groupId, topicNames);
  }

  @Override
  public KafkaConsumer<K, V> createConsumer() {
    consumer = new KafkaConsumer<K, V>(properties);
    return consumer;
  }

  @Override
  public String getName() {
    return topicNames.toString();
  }

  @Override
  public void poll() throws Throwable {

    Throwable ex = null;
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(60));
    for (TopicPartition partition : records.partitions()) {
      List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
      try {
        if (partitionRecords.size() > 0) {
          // logger.info("Processing {} events from {}",
          // partitionRecords.size(), partition.topic());
          this.handleEvents(partition, partitionRecords);
          if (!isCommitted()) {
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1))); // error
          }
        }

      } catch (Throwable e) {
        if (ex == null) {
          ex = e;
        }
        try {
          consumer.seek(partition, partitionRecords.get(0).offset()); // like rollback
        } catch (Throwable e1) {
          restartBeforeNextPoll();
        }
      }
    }

    if (ex != null) {
      throw ex;
    }

  }

  protected boolean isCommitted() {
    return false;
  }

  public void handleEvents(TopicPartition topicPartition, List<ConsumerRecord<K, V>> list) throws Throwable {
    for (ConsumerRecord<K, V> m : list) {
      if (isClosing()) {
        return;
      }
      handleEvent(topicPartition, m);
    }
  }

  public Properties getProperties() {
    return properties;
  }

  public void handleEvent(TopicPartition topicPartition, ConsumerRecord<K, V> message) throws Throwable {
    throw new CloudIOException("handleEvent must be implemented!");
  }

}