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

package com.demo.events;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.demo.input.Record;
import com.demo.util.Util;

public abstract class Consumer extends BaseConsumer<String, String> {
  private static Logger logger = LogManager.getLogger(Consumer.class);
  private boolean commitOffsets = true;

  public Consumer(String groupId, Collection<String> topicNames) {
    super(groupId, topicNames);
  }

  public Consumer(String groupId, Pattern topicPattern) {
    super(groupId, topicPattern);
  }

  public Consumer(String groupId, String topicName) {
    this(groupId, Collections.singleton(topicName));
  }

  @Override
  protected KafkaConsumer<String, String> createConsumer() {
    return new KafkaConsumer<String, String>(properties);
  }

  public void handleMessage(Record message) throws Throwable {
    throw new RuntimeException("handleMessage must be implemented!");
  }

  public void handleMessages(TopicPartition topicPartition, List<Record> list) throws Throwable {
    for (Record m : list) {
      try {
        handleMessage(m);
      } catch (Throwable e) {
        // IO.handleError(topicPartition.topic(), m, e);
        Producer.sendMessage("error_topic", m);
      }
    }
  }

  public boolean isCommitOffsets() {
    return commitOffsets;
  }

  @Override
  public void poll() throws Throwable {
    poll(60);
  }

  public final void poll(int waitSeconds) throws Throwable {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(waitSeconds));
    if (records.count() > 0) {
      processRecords(records);
    }
  }

  protected final void processRecords(ConsumerRecords<String, String> records) throws Throwable {
    Throwable ex = null;
    for (TopicPartition partition : records.partitions()) {
      List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
      if (partitionRecords.size() == 0) {
        continue;
      }
      if (logger.isInfoEnabled()) {
        logger.info("{}: Got {} events between {} & {} in {}", getName(),
            partitionRecords.size(),
            partitionRecords.get(0).offset(),
            partitionRecords.get(partitionRecords.size() - 1).offset(),
            partition.toString());
      }

      List<Record> list = new ArrayList<>(partitionRecords.size());
      for (ConsumerRecord<String, String> record : partitionRecords) {
        String message = record.value();
        if (message == null) {
          continue;
        }
        Record re = Util.getRecord(message);
        list.add(re);
      }

      try {
        if (list.size() > 0) {
          this.handleMessages(partition, list);
        }

        if (partitionRecords.size() > 0 && isCommitOffsets()) {
          long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
          commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
        }
      } catch (WakeupException | InterruptException e) {
        throw e;
      } catch (Throwable e) {
        logger.catching(e);
        if (ex == null) {
          ex = e;
        }
        try {
          logger.error("Seeking back {} events between {} & {} in {}",
              partitionRecords.size(),
              partitionRecords.get(0).offset(),
              partitionRecords.get(partitionRecords.size() - 1).offset(),
              partition.toString());

          consumer.seek(partition, partitionRecords.get(0).offset());
        } catch (Throwable e1) {
          restartBeforeNextPoll();
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  public void setCommitOffsets(boolean commitOffsets) {
    this.commitOffsets = commitOffsets;
  }

}
