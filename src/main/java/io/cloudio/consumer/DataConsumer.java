
package io.cloudio.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.task.BaseTask;
import io.cloudio.task.Data;
import io.cloudio.util.JsonDeserializer;

public class DataConsumer extends Consumer<String, Data> {
  private CountDownLatch countDownLatch = new CountDownLatch(1);
  private static Logger logger = LogManager.getLogger(BaseConsumer.class);
  final Duration initialDuration = Duration.ofSeconds(1);
  Duration duration = initialDuration;
  BaseTask<String, Data> baseTask;
  TopicPartition partition = null;
  Integer offset = null;

  public DataConsumer(String groupId, Collection<String> topicNames, BaseTask<String, Data> baseTask,
      TopicPartition partition, Integer offset) {
    super(groupId, topicNames);
    this.baseTask = baseTask;
    this.partition = partition;
    this.offset = offset;
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

  public Properties getProperties() {
    return properties;
  }

  public void error(Throwable e) {
    close();
    onError(e);
    countDownLatch.countDown();
  }

  protected void onError(Throwable e) {
    logger.catching(e);
  }

  public final void complete() {
    close();
    countDownLatch.countDown();
  }

  public void await() throws InterruptedException {
    countDownLatch.await();
  }

  @Override
  public void subscribe() {
    if (partition != null && offset > -1) {
      consumer.assign(Arrays.asList(partition));
      consumer.seek(partition, offset);
    } else {
      super.subscribe();
    }

  }

  @Override
  public void handleEvents(TopicPartition topicPartition, List<ConsumerRecord<String, Data>> records) throws Throwable {

    if (isClosing()) {
      return;
    }
    List<Data> list = new ArrayList<>(records.size());
    for (ConsumerRecord<String, Data> record : records) {
      Data data = record.value();
      if (data == null) {
        continue;
      }
      list.add(data);
    }
    this.baseTask.handleData(list);
  }

  public void seek(TopicPartition tp, long offset) {
    consumer.seek(tp, offset);
  }

  protected void updateProgress(int emptyCounter, long seconds) {
    if (logger.isDebugEnabled()) {
      logger.debug("Got empty rows for {} polls. Will try again in {} seconds!", emptyCounter, seconds);
    }
  }
}
