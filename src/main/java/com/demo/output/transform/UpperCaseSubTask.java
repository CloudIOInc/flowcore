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

package com.demo.output.transform;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.demo.events.BaseConsumer;
import com.demo.events.CloudIOException;
import com.demo.events.Notification;
import com.demo.events.Producer;
import com.demo.events.StringUtil;
import com.demo.messages.EventRequest;
import com.demo.messages.KafkaUtil;
import com.demo.messages.Record;
import com.demo.messages.Topics;

public class UpperCaseSubTask implements Runnable {
  static Logger logger = LogManager.getLogger(UpperCaseSubTask.class);

  private KafkaConsumer<String, Record> kafkaConsumer;

  TopicPartition topicPartition;
  List<Record> list;
  private CompletableFuture<Boolean> future;
  protected boolean failed = false;
  UpperCaseTransformEventRequest message;

  public UpperCaseSubTask(TopicPartition topicPartition, List<Record> list,
      UpperCaseTransformEventRequest message) {
    this.list = list;
    this.topicPartition = topicPartition;
    this.message = message;

    kafkaConsumer = new KafkaConsumer<>(BaseConsumer.getProperties(Topics.UPPERCASE_GROUP_ID));
    List<String> topics = new ArrayList<String>();
    topics.add(message.getFromTopic());

  }

  public void processMessages(TopicPartition topicPartition, List<Record> list) throws Throwable {
    int size = list.size();
    logger.debug("Started processing message from - {}", topicPartition.topic());
    if (list.size() == 0) {
      logger.debug("Skipped {} dumped events", size);
      return;
    }

    if (size != list.size()) {
      logger.debug("Skipped {} of {} dumped events", size - list.size(), size);
    }
    String dataTopic = message.getToTopic();
    try {
      for (Record r : list) {
        processData(dataTopic, r);
      }

    } catch (Throwable e) {
      /// ((CloudIOException) e).setErrorType("Output");
      logger.catching(e);
      //
      Notification.error("UpperCase", "Transform",
          StringUtil.format("Failed to post {} event to {}", message.getSettings().getType(), message.getToTopic()),
          null);
      if (list.size() > 0) {
        //            list.forEach(m -> {
        //              //   IO.handleError(topicPartition.topic(), m, e);
        //            });
        Producer.sendMessages("errors", list);
      }
      return;
    }
  }

  public void processData(String dataTopic, Record r) throws Exception {
    //  Map<String, Integer> startOffsets = KafkaUtil.getOffsets(message.getToTopic(), groupId, topicPartition, true);
    initConsumer(message, r.getAsString("topic"), r.getInteger("offset"));
    while (true) {
      ConsumerRecords<String, Record> records = kafkaConsumer.poll(Duration.ofMillis(10000));
      if (records.count() == 0) {
        // postEvent(startOffsets);
        break;
      }
      try (Producer producer = Producer.get()) {
        List<Record> dataRecs = new ArrayList<Record>();
        ConsumerRecord<String, Record> lastRecord = null;
        for (ConsumerRecord<String, Record> record : records) {

          try {
            Record message = record.value();
            // logger.info("Received message: " + message);
            dataRecs.add(message);
            lastRecord = record;
            transform(producer, message);
            // logger.info("After transformation message: " + message);
          } catch (Exception e) {
            logger.error(e.getMessage());
          }
          Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

          commitMessage.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()),
              new OffsetAndMetadata(lastRecord.offset() + 1));

          kafkaConsumer.commitSync(commitMessage);
        }

        logger.info("Transfomred - {} data successfully!", dataRecs.size());
      }
    }
  }

  private void postEvent(Map<String, Integer> startOffsets) {
    // TODO Auto-generated method stub

  }

  private void transformInternal(List<String> fields, Record r) {
    fields.forEach(field -> {
      String str = r.getAsString(field);
      if (str != null) {
        r.set(field, str.toUpperCase());
      }
    });

  }

  public void transform(Producer producer, Record rec) throws Exception {
    try {
      producer.beginTransaction();
      List<String> fields = message.getSettings().getTransforFields();
      boolean isTransorm = fields != null && fields.size() > 0;
      if (isTransorm) {
        transformInternal(fields, rec);
      }
      producer.send(message.getToTopic(), rec);
      producer.commitTransaction();
    } catch (Exception e) {
      logger.catching(e);
      throw new CloudIOException("Error while processing data - {}", rec.toString());
    }

  }

  private boolean execute() throws Throwable {
    processMessages(topicPartition, list);
    return true;
  }

  @Override
  public void run() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Running task: {}", topicPartition.toString());
      }
      boolean status = execute();
      if (logger.isDebugEnabled()) {
        logger.debug("Done running task: {}", topicPartition.toString());
      }
      future.complete(status);
    } catch (Throwable e) {
      failed = true;
      logger.error("Error running task: {}", topicPartition.toString());
      logger.catching(e);
      future.completeExceptionally(e);
    }
  }

  public CompletableFuture<Boolean> start() {
    if (logger.isDebugEnabled()) {
      logger.debug("Starting task: {}", topicPartition.toString());
    }
    future = new CompletableFuture<Boolean>();
    KafkaUtil.execute(this);
    return future;
  }

  public void initConsumer(EventRequest<?, ?> message,
      String assignedPartition, Integer offset) {
    String part = assignedPartition.substring(assignedPartition.lastIndexOf("-") + 1);
    int partition = Integer.parseInt(part);
    TopicPartition tp = new TopicPartition(message.getFromTopic(), partition);
    List<TopicPartition> _tps = new ArrayList<TopicPartition>();
    _tps.add(tp);
    kafkaConsumer.assign(_tps);
    // kafkaConsumer.assignment();
    kafkaConsumer.seek(tp, offset);
  }
}
