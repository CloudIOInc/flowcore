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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import com.demo.util.JsonDeserializer;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Monitor;

import io.cloudio.scale.kafka.eventbus.BusyState;
import io.cloudio.scale.kafka.eventbus.IOEventBus;
import io.cloudio.scale.kafka.eventbus.NodeStateEvent;
import io.cloudio.scale.kafka.zookeeper.ZookeeperCache;
import io.cloudio.scale.kafka.zookeeper.ZookeeperCacheEvent;

public abstract class BaseConsumer<K, V> implements Runnable, Resubscribe {
  private static Logger logger = LogManager.getLogger(BaseConsumer.class);

  public static Properties getProperties(String groupId) {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    if (groupId != null) {
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 300);
    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600_000);
    properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
    properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 2_000_000);
    properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 2_000_000);
    properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    properties.put("internal.leave.group.on.close", true);

    return properties;
  }

  private volatile boolean closed = false;
  protected KafkaConsumer<K, V> consumer;
  private long errorCount = 0;
  protected String groupId;
  private CountDownLatch latch = null;
  protected final Monitor monitor = new Monitor();
  private AtomicBoolean pause = new AtomicBoolean(false);
  private AtomicBoolean paused = new AtomicBoolean(false);
  private Collection<TopicPartition> pausedPartitions;
  private final Set<TopicPartition> toBePaused = new HashSet<>();
  private final Set<TopicPartition> pausedSet = new HashSet<>();
  private final Set<TopicPartition> toBeResumed = new HashSet<>();
  private final Map<TopicPartition, OffsetAndMetadata> toBeCommitted = new HashMap<>();
  private final Map<TopicPartition, Long> toBeSeekedOnResume = new HashMap<>();
  protected Properties properties;
  private AtomicBoolean restart = new AtomicBoolean(false);
  private AtomicBoolean resubscribe = new AtomicBoolean(false);
  private AtomicBoolean resume = new AtomicBoolean(false);
  protected Collection<String> topicNames;
  protected Pattern topicPattern;
  public static final ZookeeperCache<JobConsumerStatus> partitionLockCache = new ZookeeperCache<JobConsumerStatus>(
      "/cloudio/flow/partitionLocks");
  protected BusyState busyState;

  public BaseConsumer(String groupId) {
    this.groupId = groupId;
    this.properties = getProperties(groupId);
    this.properties.put(ConsumerConfig.CLIENT_ID_CONFIG, getClientId());
    if (isPausable()) {
      busyState = new BusyState(getName());
      IOEventBus.register(this);
    }
  }

  public BaseConsumer(String groupId, Collection<String> topicNames) {
    this(groupId);
    this.topicNames = topicNames;
  }

  public BaseConsumer(String groupId, Pattern topicPattern) {
    this(groupId);
    this.topicPattern = topicPattern;
  }

  protected void afterPause(Collection<TopicPartition> partitions) {
    // override
  }

  protected void afterResume(Collection<TopicPartition> partitions) {
    // override
  }

  protected void beforePause(Collection<TopicPartition> partitions) {
    // override
  }

  protected void beforeResume(Collection<TopicPartition> partitions) {
    // override
  }

  protected boolean canRun() {
    return consumer != null && !closed;
  }

  private void doCommitBeforeNextPoll(Collection<TopicPartition> list) {
    if (toBeCommitted.size() == 0 || list.size() == 0) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> assigned = new HashMap<>();
    Map<TopicPartition, OffsetAndMetadata> revoked = new HashMap<>();
    toBeCommitted.forEach((partition, offset) -> {
      if (list.contains(partition)) {
        assigned.put(partition, offset);
      } else {
        revoked.put(partition, offset);
      }
    });
    boolean freeup = true;
    if (assigned.size() > 0) {
      try {
        commitSync(assigned);
        assigned.forEach((tp, om) -> {
          toBeCommitted.remove(tp);
          new RetryableFunction("Unlock Job").call(() -> {
            partitionLockCache.put(tp.toString(),
                new JobConsumerStatus(getName(), om.metadata(), JobConsumerStatus.Status.Complete,
                    -1, om.offset() - 1));
          });
        });
      } catch (RebalanceInProgressException e) {
        freeup = false;
        logger.warn("{}: Deferring commit of assigned partitions {} due to RebalanceInProgressException!", getName(),
            this.tpCollectionToString(assigned.keySet()));
      } catch (Exception e) {
        // ignore
        logger.error("{}: Error commiting assigned partitions", this.getName());
        logger.catching(e);
      }
    }
    if (revoked.size() > 0) {
      try {
        commitSync(revoked);
        revoked.forEach((tp, om) -> {
          toBeCommitted.remove(tp);
          new RetryableFunction("Unlock Job").call(() -> {
            partitionLockCache.put(tp.toString(),
                new JobConsumerStatus(getName(), om.metadata(), JobConsumerStatus.Status.Complete,
                    -1, om.offset() - 1));
          });
        });
      } catch (RebalanceInProgressException e) {
        freeup = false;
        logger.warn("{}: Deferring commit of revoked partitions {} due to RebalanceInProgressException!", getName(),
            this.tpCollectionToString(revoked.keySet()));
      } catch (Exception e) {
        // ignore
        revoked.forEach((tp, om) -> {
          toBeCommitted.remove(tp);
          new RetryableFunction("Unlock Job").call(() -> {
            partitionLockCache.put(tp.toString(),
                new JobConsumerStatus(getName(), om.metadata(), JobConsumerStatus.Status.Complete,
                    -1, om.offset() - 1));
          });
        });
        logger.error("{}: Error commiting revoked partitions", this.getName());
        logger.catching(e);
      }
    }
    if (busyState != null && freeup) busyState.checkAndFree();
  }

  private void checkToBePausedOrResumed() {
    monitor.enter();
    try {
      Set<TopicPartition> assignments = consumer.assignment();
      doCommitBeforeNextPoll(assignments);
      Iterator<TopicPartition> itr = toBeResumed.iterator();
      while (itr.hasNext()) {
        TopicPartition tp = itr.next();
        if (toBePaused.contains(tp)) {
          logger.debug("{}: Waiting to be paused, hence not resuming {}", getName(), tp.toString());
          itr.remove();
        }
        if (!assignments.contains(tp)) {
          logger.debug("{}: Not assigned anymore, hence not resuming {}", getName(), tp.toString());
          itr.remove();
        }
      }
      if (toBeResumed.size() > 0) {
        if (!pause.get() && !paused.get()) {
          logger.info("{}: Resuming {}", getName(), tpCollectionToString(toBeResumed));
          consumer.resume(toBeResumed);
          toBeResumed.forEach(tp -> {
            if (toBeSeekedOnResume.containsKey(tp)) {
              consumer.seek(tp, toBeSeekedOnResume.get(tp));
              toBeSeekedOnResume.remove(tp);
            }
          });
        }
        toBeResumed.forEach(tp -> {
          pausedSet.remove(tp);
        });
        toBeResumed.clear();
      }

      if (toBePaused.size() > 0) {
        logger.info("{}: Pausing {}", getName(), tpCollectionToString(toBePaused));
        consumer.pause(toBePaused);
        pausedSet.addAll(toBePaused);
        toBePaused.clear();
      }
    } finally {
      monitor.leave();
    }
  }

  private void checkPauseOrResume() {
    checkToBePausedOrResumed();
    if (!isPausable()) {
      return;
    }
    monitor.enter();
    try {
      if (pause.compareAndSet(true, false)) {
        pauseConsumer();
        paused.set(true);
      }
      if (resume.compareAndSet(true, false)) {
        resumeConsumer();
        paused.set(false);
      }
    } finally {
      monitor.leave();
    }
  }

  private void pauseConsumer() {
    logger.info("{}: Pausing consumer", getName());
    pausedPartitions = consumer.assignment();
    beforePause(pausedPartitions);
    consumer.pause(pausedPartitions);
    afterPause(pausedPartitions);
    if (busyState != null) busyState.leaveAndFree();
  }

  private void resumeConsumer() {
    logger.info("{}: Resuming consumer", getName());
    if (busyState != null) busyState.enter();
    Set<TopicPartition> assignment = consumer.assignment();
    Set<TopicPartition> activePausedPartitions = pausedPartitions.stream()
        .filter(tp -> !pausedSet.contains(tp) && assignment.contains(tp))
        .collect(Collectors.toSet());
    beforeResume(activePausedPartitions);
    consumer.resume(activePausedPartitions);
    afterResume(activePausedPartitions);
    pausedPartitions = null;
  }

  private void checkRestart() {
    if (restart.compareAndSet(true, false)) {
      closeQuietly(consumer);
      consumer = createConsumer();
      resubscribe.set(true);
    }
  }

  private void checkResubscribe() {
    if (resubscribe.compareAndSet(true, false)) {
      subscribe();
    }
  }

  public static final void closeQuietly(AutoCloseable autoClosable) {
    if (autoClosable != null) {
      try {
        autoClosable.close();
      } catch (Throwable e) {
      }
    }
  }

  @Override
  public void close() {
    closed = true;
    wakeup();
  }

  protected abstract KafkaConsumer<K, V> createConsumer();

  public String getClientId() {
    return "consumbers_" + "-" + this.getClass().getSimpleName();
  }

  public abstract String getName();

  protected void handleOnPartitionsAssigned(Collection<TopicPartition> partitions) {

  }

  private String tpCollectionToString(Collection<TopicPartition> partitions) {
    return partitions.stream().map(tp -> tp.toString()).collect(Collectors.joining(", "));
  }

  protected void handleOnPartitionsRevoked(Collection<TopicPartition> partitions) {

  }

  protected boolean isClosing() {
    return closed;
  }

  public boolean isConsumerPaused() {
    return paused.get();
  }

  protected boolean isPausable() {
    return true;
  }

  @Subscribe
  public void onEvent(NodeStateEvent event) {
    monitor.enter();
    try {
      if (event.isPauseEvent()) {
        if (resume.get()) {
          // pending resume... just cancel it
          resume.set(false);
        } else {
          pause.compareAndSet(false, true);
          wakeup();
        }
      } else if (event.isResumeEvent()) {
        if (pause.get()) {
          // pending pause... just cancel it
          pause.set(false);
        } else {
          resume.compareAndSet(false, true);
          wakeup();
        }
      }
    } finally {
      monitor.leave();
    }
  }

  protected abstract void poll() throws Throwable;

  protected void restartBeforeNextPoll() {
    restart.set(true);
  }

  @Override
  public void resubscribe() {
    this.resubscribe.compareAndSet(false, true);
  }

  @Override
  public final void run() {
    if (consumer != null) {
      throw CloudIOException.with("{}: Consumer already running!", getName());
    }
    ThreadContext.put("instanceIdentifier", "[consumers]");
    logger.info("{}: Starting Consumer...", getName());

    try {
      if (busyState != null) {
        busyState.enter();
      }
      partitionLockCache.start();
      consumer = createConsumer();
      subscribe();
      while (canRun()) {
        try {
          checkRestart();

          checkResubscribe();

          checkPauseOrResume(); // this is throwing WakeupException when trying to commit the offsets before
                               // resume

          poll();
          errorCount = 0;
        } catch (WakeupException | InterruptException e) {
          // logger.warn("{} wokeup/interrupted...", getName());
        } catch (Throwable e) {
          errorCount++;
          logger.catching(e);
          Notification.error("Consumer", getName(), ErrorHandler.getMessage(e), ErrorHandler.getMessageStackHTML(e));
          if (errorCount > 10) {
            Notification.error("Consumer", getName(), "Too many errors! Closing consumer " + getName(), null);
            throw e;
          }
        }
      }
      if (consumer != null) {
        // commit any pending commits in toBeCommitted
        Set<TopicPartition> assignments = consumer.assignment();
        doCommitBeforeNextPoll(assignments);
      }
    } catch (Throwable e) {
      logger.catching(e);
    } finally {
      logger.warn("Closing {} Consumer", getName());
      closeQuietly(consumer);
      logger.warn("Closed {} Consumer", getName());
      if (isPausable()) {
        IOEventBus.unregister(this);
        busyState.leaveAndFree();
      }
    }

  }

  public void setCountDownLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  public void pauseBeforeNextPoll(TopicPartition tp) {
    monitor.enter();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: pauseBeforeNextPoll: {}", getName(), tp.toString());
      }
      toBePaused.add(tp);
    } finally {
      monitor.leave();
    }
  }

  public void resumeBeforeNextPoll(TopicPartition tp) {
    monitor.enter();
    try {
      if (toBePaused.contains(tp)) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Skipping resumeBeforeNextPoll as it's part of toBePaused: {}", getName(), tp.toString());
        }
        toBePaused.remove(tp);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: resumeBeforeNextPoll: {}", getName(), tp.toString());
        }
        toBeResumed.add(tp);
      }
    } finally {
      monitor.leave();
    }
  }

  public boolean isPendingCommit(TopicPartition tp) {
    monitor.enter();
    try {
      return toBeCommitted.containsKey(tp);
    } finally {
      monitor.leave();
    }
  }

  public void commitBeforeNextPoll(TopicPartition tp, OffsetAndMetadata offset) {
    monitor.enter();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: commitBeforeNextPoll: {} - {}", getName(), tp.toString(), offset.offset());
      }
      toBeCommitted.put(tp, offset);
    } finally {
      monitor.leave();
    }
  }

  private void onPartitionLockChanged(String key, ZookeeperCacheEvent<JobConsumerStatus> lockStatus) {
    try {
      // remove the node to create a new EPHEMERAL node on put later
      partitionLockCache.remove(key);
      if (!lockStatus.isDelete()) {
        if (lockStatus.value.status != JobConsumerStatus.Status.Running) {
          monitor.enter();
          try {
            int index = key.lastIndexOf('-');
            String topic = key.substring(0, index);
            int partition = Integer.parseInt(key.substring(index + 1));
            TopicPartition tp = new TopicPartition(topic, partition);
            logger.warn("{} Resuming {} as the job {} is now complete. Seeking: {}", getName(), tp.toString(),
                lockStatus.value.toString(), lockStatus.value.endOffset + 1);
            toBeResumed.add(tp);
            toBeSeekedOnResume.put(tp, lockStatus.value.endOffset + 1);
            consumer.wakeup();
          } finally {
            monitor.leave();
          }
        }
      } else {
        int index = key.lastIndexOf('-');
        String topic = key.substring(0, index);
        int partition = Integer.parseInt(key.substring(index + 1));
        TopicPartition tp = new TopicPartition(topic, partition);
        if (lockStatus.oldValue == null) {
          logger.warn("{} Key {} Missing oldValue... this should never happen!",
              getName(),
              tp.toString());
        } else {
          monitor.enter();
          try {
            toBeResumed.add(tp);
            if (lockStatus.oldValue.status == JobConsumerStatus.Status.Complete) {
              logger.warn("{} Resuming {} as the job {} is now complete. Seeking: {}", getName(), tp.toString(),
                  lockStatus.value.toString(), lockStatus.oldValue.endOffset + 1);
              toBeSeekedOnResume.put(tp, lockStatus.oldValue.endOffset + 1);
            } else {
              logger.warn(
                  "{} Resuming {} and re-running job {} as the previous consumer unexpectedly disconnected! Seeking: {}",
                  getName(), tp.toString(), lockStatus.oldValue.toString(), lockStatus.oldValue.startOffset);
              toBeSeekedOnResume.put(tp, lockStatus.oldValue.startOffset);
            }
            consumer.wakeup();
          } finally {
            monitor.leave();
          }
        }
      }
    } catch (Exception e) {
      logger.catching(e);
    }
  }

  protected void subscribe() {
    ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.size() > 0) {
          doCommitBeforeNextPoll(consumer.assignment());
          partitions.forEach(tp -> {
            ZookeeperCacheEvent<JobConsumerStatus> lockStatus = partitionLockCache.get(tp.toString());
            if (lockStatus != null) {
              if (!lockStatus.isDelete()) {
                if (lockStatus.value.status == JobConsumerStatus.Status.Running) {
                  logger.warn("{} Pausing {} as {} is still running", getName(), tp.toString(),
                      lockStatus.value.toString());
                  partitionLockCache.addCallbackOnceListener(tp.toString(), BaseConsumer.this::onPartitionLockChanged);
                  consumer.pause(Collections.singleton(tp));
                  pausedSet.add(tp);
                } else {
                  // remove the node to create a new EPHEMERAL node on put later
                  partitionLockCache.remove(tp.toString());
                }
              } else {
                // remove the node to create a new EPHEMERAL node on put later
                partitionLockCache.remove(tp.toString());
                if (lockStatus.oldValue == null) {
                  logger.warn("{} Key {} Missing oldValue... this should never happen!",
                      getName(),
                      tp.toString());
                } else {
                  if (lockStatus.oldValue.status == JobConsumerStatus.Status.Complete) {
                    logger.warn(
                        "{} Resuming {} as the job {} is now complete and the previous consumer unexpectedly disconnected. Seeking: {}",
                        getName(), tp.toString(), lockStatus.oldValue.toString(), lockStatus.oldValue.endOffset + 1);
                    consumer.seek(tp, lockStatus.oldValue.endOffset + 1);
                  } else {
                    logger.warn(
                        "{} Re-running job {} from {} as the previous consumer unexpectedly disconnected! Seeking: {}",
                        getName(), lockStatus.oldValue.toString(), tp.toString(), lockStatus.oldValue.startOffset);
                    consumer.seek(tp, lockStatus.oldValue.startOffset);
                  }
                }
              }
            }
          });
        }
        if (latch != null) {
          latch.countDown();
          latch = null;
        }
        if (logger.isWarnEnabled()) {
          logger.warn("{} --- onPartitionsAssigned: {}", getName(), tpCollectionToString(partitions));
          logger.warn("{} --- Current Assignments: {}", getName(), tpCollectionToString(consumer.assignment()));
        }
        // StatusMonitor.onPartitionsAssigned(partitions);
        handleOnPartitionsAssigned(partitions);
      }

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // if (!IOSystem.isShuttingDown()) {
        logger.warn("{} --- onPartitionsRevoked: {}", getName(), tpCollectionToString(partitions));
        logger.warn("{} --- Current Assignments: {}", getName(), tpCollectionToString(consumer.assignment()));
        // }
        handleOnPartitionsRevoked(partitions);
      }

      @Override
      public void onPartitionsLost(Collection<TopicPartition> partitions) {
        logger.info("{}: onPartitionsLost", getName());
        //        if (!IOSystem.isShuttingDown()) {
        //          logger.warn("{} --- onPartitionsLost: {}", getName(), tpCollectionToString(partitions));
        //        }
        //StatusMonitor.onPartitionsRevoked(partitions);
        handleOnPartitionsRevoked(partitions);
      }
    };
    if (topicPattern != null) {
      consumer.subscribe(topicPattern, consumerRebalanceListener);
    } else {
      consumer.subscribe(topicNames, consumerRebalanceListener);
    }
  }

  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    try {
      // commitSync may throw WakeupException, when trying to commit before poll after a wakeup() call
      consumer.commitSync(offsets);
    } catch (WakeupException | InterruptException ex1) {
      try {
        consumer.commitSync(offsets);
      } catch (WakeupException | InterruptException ex2) {
        consumer.commitSync(offsets);
      }
    }
  }

  protected void wakeup() {
    if (consumer != null) {
      consumer.wakeup();
    }
  }

}
