/*
 * Copyright (c) 2014 - present CloudIO Inc.
 * 1248 Reamwood Ave, Sunnyvale, CA 94089
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * CloudIO Inc. ("Confidential Information").  You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with CloudIO.
 */

package io.cloudio.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.task.BaseTask;
import io.cloudio.util.CloudIOException;
import io.cloudio.util.Util;

public abstract class SinglePartitionEventConsumer<K, V> extends Consumer<K, V> {
  private static Logger logger = LogManager.getLogger(SinglePartitionEventConsumer.class);
  private CountDownLatch countDownLatch = new CountDownLatch(1);
  TopicPartition partition;
  long counter = 0;
  protected BaseTask<K, V> task;
  private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  final Duration initialDuration = Duration.ofSeconds(1);
  Duration duration = initialDuration;
  private int emptyCounter = 0;

  @Override
  protected void afterPause(Collection<TopicPartition> partitions) {
    error(new CloudIOException("Consumer paused in the middle of processing!"));
  }

  @Override
  public String getClientId(String groupId) {
    return Util.getInsatnceIdentifier() + "-SinglePartitionEventConsumer-"
        + CONSUMER_CLIENT_ID_SEQUENCE.incrementAndGet();
  }

  public SinglePartitionEventConsumer(String groupId, TopicPartition partition, BaseTask<K, V> baseTask) {
    super(groupId, Arrays.asList(partition.topic()));
    this.partition = partition;
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
    properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
    this.task = baseTask;
  }

  @Override
  public String getName() {
    return "Single Partition-" + topicNames.toString();
  }

  public void await() throws InterruptedException {
    countDownLatch.await();
  }

  @Override
  public void poll() throws Exception {
    if (isClosing()) {
      return;
    }
    counter++;
    ConsumerRecords<K, V> records = consumer.poll(duration);
    if (records.count() == 0) {
      emptyCounter++;
      if (emptyCounter % 10 == 0) {
        logger.info("Got empty records for {} polls", emptyCounter);
      }
      if (duration.getSeconds() < 600) {
        duration = duration.plusSeconds(duration.getSeconds());
      }
      if (duration.getSeconds() > 600) {
        duration = Duration.ofSeconds(600);
      }
      return;
    } else {
      emptyCounter = 0;
      duration = initialDuration;
    }

    List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);

    if (partitionRecords.size() > 0) {
      logger.info("found records: " + partitionRecords.size());
      try {
        this.handleEvents(partition, partitionRecords);
      } catch (Throwable e) {
        logger.catching(e);
        error(e);
      }
    }
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

}
