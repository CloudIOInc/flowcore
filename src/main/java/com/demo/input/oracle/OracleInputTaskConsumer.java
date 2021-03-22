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

package com.demo.input.oracle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.demo.events.CloudIOException;
import com.demo.events.Consumer;
import com.demo.events.JobConsumerStatus;
import com.demo.messages.Record;

public class OracleInputTaskConsumer extends Consumer {
  private static Logger logger = LogManager.getLogger(OracleInputTaskConsumer.class);
  private Map<TopicPartition, OracleSubTask2> pendingTaskMap = new ConcurrentHashMap<>();
  private DataSource ds;

  public OracleInputTaskConsumer(DataSource ds) {
    super("oracle_sub_task", "oralce_input_task_topic");
    // offset commits are handled by this class
    // hence turn off the default commit in parent class Consumer
    setCommitOffsets(false);
    this.ds = ds;
  }

  @Override
  public String getName() {
    return "OracleInput";
  }

  @Override
  public void handleMessages(final TopicPartition topicPartition, final List<Record> list) throws Throwable {
    // this method is invoked from Consumer.processRecords inside a loop, once per topicPartition
    // hence need to handle multiple tasks in a single poll
    if (list.isEmpty()) {
      return;
    }
    final CompletableFuture<Boolean> future;
    final OracleSubTask2 task;
    final Record firstMessage = list.get(0);
    final Record lastMessage = list.get(list.size() - 1);
    // add start offset, end offset
    // success = endoffset + 1
    final long startOffset = firstMessage.getLong("offset");
    final long endOffset = firstMessage.getLong("offset");

    monitor.enter();
    try {
      OracleSubTask2 pendingTask = pendingTaskMap.get(topicPartition);
      if (pendingTask != null) {
        // this should never happen
        throw CloudIOException.with(
            "Task already in progress for {}, hence cannot process {} messages from {}",
            pendingTask.topicPartition.toString(), list.size(), topicPartition.toString());
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Creating task to process {} messages from {}, already running task count: {}", list.size(),
              topicPartition.toString(), pendingTaskMap.size());
        }
      }
      partitionLockCache.put(topicPartition.toString(),
          new JobConsumerStatus(getName(), lastMessage.getAsString("wfNodeInstanceId"),
              JobConsumerStatus.Status.Running,
              startOffset, endOffset));
      task = new OracleSubTask2(topicPartition, list, ds);
      pauseBeforeNextPoll(topicPartition);
      pendingTaskMap.put(topicPartition, task);
    } finally {
      monitor.leave();
    }
    if (busyState != null) busyState.enter();
    future = task.start();
    future.handle((Boolean r, Throwable ex) -> {
      monitor.enter();
      try {
        if (busyState != null) busyState.leave();
        commitBeforeNextPoll(topicPartition, new OffsetAndMetadata(endOffset + 1, lastMessage.getAsString("wfFlowId")));
        pendingTaskMap.remove(topicPartition);
        resumeBeforeNextPoll(topicPartition);
      } finally {
        monitor.leave();
      }
      wakeup();
      return r;
    });

  }

}
