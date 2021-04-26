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
  private boolean initialized = false;
  private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  final Duration initialDuration = Duration.ofSeconds(1);
  Duration duration = initialDuration;
  private boolean endOffsetReached = false;
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
    return "Single Partition Event";
  }

  public void await() throws InterruptedException {
    countDownLatch.await();
  }

  protected void onComplete() {

  }

  protected void doFinally() {

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
      if (endOffsetReached) {
        complete();
      } else if (emptyCounter > 10) {
        error(CloudIOException.with(
            "Got empty records for 10 polls before reaching the end offset "));
      }
      if (duration.getSeconds() < 600) {
        duration = duration.plusSeconds(duration.getSeconds());
      }
      if (duration.getSeconds() > 600) {
        duration = Duration.ofSeconds(600);
      }
      updateProgress(emptyCounter, duration.getSeconds());
      return;
    } else {
      emptyCounter = 0;
      duration = initialDuration;
    }

    List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);

    if (partitionRecords.size() > 0) {
      // logger.info("found records: " + partitionRecords.size());
      try {
        if (!initialized) {
          initialized = true;
          this.initialize(partition.topic());
        }
        this.startEventBatch(partition.topic(), partitionRecords);
        // logger.info("before handleEvents");
        this.handleEvents(partition, partitionRecords);
        // logger.info("after handleEvents");
        this.endEventBatch(partition.topic(), partitionRecords);
      } catch (Throwable e) {
        logger.catching(e);
        error(e);
      }
    }
    if (endOffsetReached) {
      complete();
    }
  }

  protected void updateProgress(int emptyCounter, long seconds) {
    if (logger.isDebugEnabled()) {
      logger.debug("Got empty rows for {} polls. Will try again in {} seconds!", emptyCounter, seconds);
    }
  }

  public void error(Throwable e) {
    close();
    onError(e);
    doFinally();
    countDownLatch.countDown();
  }

  protected void onError(Throwable e) {
    logger.catching(e);
  }

  protected void initialize(String topic) throws Exception {

  }

  public final void complete() {
    close();
    onComplete();
    doFinally();
    countDownLatch.countDown();
  }

  protected void endEventBatch(String topic, List<ConsumerRecord<K, V>> partitionRecords) throws Exception {

  }

  protected void startEventBatch(String topic, List<ConsumerRecord<K, V>> partitionRecords) throws Exception {

  }

}
