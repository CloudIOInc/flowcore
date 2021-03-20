
package com.demo.events;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demo.input.Task;

public class MultithreadConsumer implements Runnable, ConsumerRebalanceListener {
  private Logger log = LoggerFactory.getLogger(MultithreadConsumer.class);

  private final KafkaConsumer<String, String> consumer;
  private final ExecutorService executor = Executors.newFixedThreadPool(8);
  private final Map<TopicPartition, Task> activeTasks = new HashMap<>();
  private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private long lastCommitTime = System.currentTimeMillis();
  //  private final Logger log = LoggerFactory.getLogger(MultithreadConsumer.class);

  public MultithreadConsumer(String topic) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "multithreaded-consumer-demo");
    consumer = new KafkaConsumer<>(config);
    new Thread(this).start();
  }

  @Override
  public void run() {
    try {
      consumer.subscribe(Collections.singleton("topic-name"), this);
      while (!stopped.get()) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        handleFetchedRecords(records);
        checkActiveTasks();
        commitOffsets();
      }
    } catch (WakeupException we) {
      if (!stopped.get())
        throw we;
    } finally {
      consumer.close();
    }
  }

  private void handleFetchedRecords(ConsumerRecords<String, String> records) {
    if (records.count() > 0) {
      List<TopicPartition> partitionsToPause = new ArrayList<>();
      records.partitions().forEach(partition -> {
        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
        Task task = new Task(partitionRecords);
        partitionsToPause.add(partition);
        executor.submit(task);
        activeTasks.put(partition, task);
      });
      consumer.pause(partitionsToPause);
    }
  }

  private void commitOffsets() {
    try {
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis - lastCommitTime > 5000) {
        if (!offsetsToCommit.isEmpty()) {
          consumer.commitSync(offsetsToCommit);
          offsetsToCommit.clear();
        }
        lastCommitTime = currentTimeMillis;
      }
    } catch (Exception e) {
      log.error("Failed to commit offsets!", e);
    }
  }

  private void checkActiveTasks() {
    List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
    activeTasks.forEach((partition, task) -> {
      if (task.isFinished())
        finishedTasksPartitions.add(partition);
      long offset = task.getCurrentOffset();
      if (offset > 0)
        offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
    });
    finishedTasksPartitions.forEach(partition -> activeTasks.remove(partition));
    consumer.resume(finishedTasksPartitions);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    //  1. Stop all tasks handling records from revoked partitions
    Map<TopicPartition, Task> stoppedTask = new HashMap<>();
    for (TopicPartition partition : partitions) {
      Task task = activeTasks.remove(partition);
      if (task != null) {
        task.stop();
        stoppedTask.put(partition, task);
      }
    }

    //  2. Wait for stopped tasks to complete processing of current record
    stoppedTask.forEach((partition, task) -> {
      long offset = task.waitForCompletion();
      if (offset > 0)
        offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
    });

    //  3. collect offsets for revoked partitions
    Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
    partitions.forEach(partition -> {
      OffsetAndMetadata offset = offsetsToCommit.remove(partition);
      if (offset != null)
        revokedPartitionOffsets.put(partition, offset);
    });

    //  4. commit offsets for revoked partitions
    try {
      consumer.commitSync(revokedPartitionOffsets);
    } catch (Exception e) {
      log.warn("Failed to commit offsets for revoked partitions!");
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    consumer.resume(partitions);
  }

  public void stopConsuming() {
    stopped.set(true);
    consumer.wakeup();
  }

}