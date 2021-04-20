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

package io.cloudio.producer;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.task.Data;
import io.cloudio.util.JsonSerializer;

public class Producer extends KafkaProducer<String, Object> implements TransactionProducer<String, Object> {
  static Logger logger = LogManager.getLogger(Producer.class);
  static final Object mutex = new Object();
  static final Deque<Producer> pool = new ArrayDeque<Producer>();
  static final List<Producer> openPool = new ArrayList<>();
  static AtomicLong counter = new AtomicLong(0);
  boolean firstSend = true;
  private final long createdAt = System.currentTimeMillis();

  private static Producer verifyAndGet() {
    synchronized (mutex) {
      Producer p = null;
      if (!pool.isEmpty()) {
        p = pool.pop();
        long now = System.currentTimeMillis();
        if (now - p.createdAt > 60000) {
          p.superClose();
          if (logger.isDebugEnabled()) {
            logger.debug("Recreating new Producer w/ TxnId: {}", p.txnId);
          }
          p = createNewProducer(p.txnId);
        }
        openPool.add(p);
      }
      return p;
    }
  }

  public static Producer get() {
    Producer p = verifyAndGet();
    if (p != null) {
      return p;
    }

    p = createNewProducer();
    synchronized (mutex) {
      openPool.add(p);
    }
    return p;
  }

  private static Producer createNewProducer() {
    long count = counter.incrementAndGet();
    String txnId = "SB_INSTANCE_" + "_" + count;
    logger.debug("Creating new Producer w/ TxnId: {}", txnId);
    return createNewProducer(txnId);
  }

  private static Producer createNewProducer(final String txnId) {
    Properties properties = Producer.getProperties();
    properties.put("transactional.id", txnId);
    Producer p = new Producer(properties);
    p.txnId = txnId;
    p.initTransactions();
    p.txnInitialized = true;
    return p;
  }

  public static Properties getProperties() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 6_000_000); // 6 mb
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 2_000_000); // 2 mb
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 2000);
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300100);
    properties.put(ProducerConfig.RETRIES_CONFIG, 10);
    properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 10000);
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000);
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 12_000_000);
    // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
    // FlowPartitioner.class);

    return properties;
  }

  public static RecordMetadata sendMessage(String topicName, Data message) throws Exception {
    try (Producer p = get()) {
      p.beginTransaction();
      RecordMetadata result = null;
      try {
        // logger.info(message.toString());
        result = p.send(topicName, message).get();
        p.commitTransaction();
      } catch (Exception e) {
        logger.catching(e);
        p.abortTransactionQuietly();
        throw e;
      }
      return result;
    }
  }

  public static OffsetRange sendMessages(String topicName, List<Data> messages) throws Exception {
    try (Producer p = get()) {
      p.beginTransaction();

      try {
        Future<RecordMetadata> start = null, end = null;
        for (Data message : messages) {
          end = p.send(topicName, message);
          if (start == null) {
            start = end;
          }
        }
        p.commitTransaction();
        OffsetRange range = new OffsetRange();
        range.start = start.get().offset();
        range.end = end.get().offset();
        return range;
      } catch (Exception e) {
        logger.catching(e);
        p.abortTransactionQuietly();
        throw e;
      }
    }
  }

  public static void shutdown() {
    synchronized (mutex) {
      for (Producer p : pool) {
        p.superClose();
      }
      pool.clear();
      for (Producer p : openPool) {
        p.abortTransactionQuietly();
        p.superClose();
      }
      openPool.clear();
    }
  }

  boolean inTransaction = false;
  boolean txnInitialized = false;
  public String txnId;

  private Producer(Properties properties) {
    super(properties);
  }

  @Override
  public void abortTransactionQuietly() {
    if (this.inTransaction) {
      try {
        super.abortTransaction();
      } catch (Exception e) {
        logger.catching(e);
        // ignore me
      }
      this.inTransaction = false;
    }
  }

  @Override
  public void abortTransaction() {
    if (this.inTransaction) {
      abortTransactionQuietly();
    } else {
      throw new RuntimeException("cannot abort... not in transaction " + this.txnId);
    }
  }

  @Override
  public void beginTransaction() {
    if (this.inTransaction) {
      throw new RuntimeException("already in transaction " + this.txnId);
    }
    if (!this.txnInitialized) {
      super.initTransactions();
    }
    super.beginTransaction();
    this.inTransaction = true;
  }

  @Override
  public void close() {
    if (this.inTransaction) {
      throw new RuntimeException("closing with in transaction " + this.txnId);
    }
    synchronized (mutex) {
      // logger.warn("Closing Producer {}", this.txnId);
      pool.push(this);
      openPool.remove(this);
    }
  }

  @Override
  public void commitTransaction() {
    if (this.inTransaction) {
      commitTransactionQuietly();
    } else {
      throw new RuntimeException("cannot commit.. not in transaction " + this.txnId);
    }
  }

  @Override
  public void commitTransactionQuietly() {
    if (this.inTransaction) {
      super.commitTransaction();
      this.inTransaction = false;
    }
  }

  public Future<RecordMetadata> send(String topicName, Object message) throws Exception {
    return send(topicName, null, message);
  }

  public Future<RecordMetadata> send(String topicName, Integer partition, String key, Object message) throws Exception {
    return send(new ProducerRecord<>(topicName, partition, key, message));
  }

  public void superClose() {
    try {
      super.flush();
      super.close(Duration.ofMillis(300));
    } catch (Throwable e) {
      logger.catching(e);
    }
  }

  @Override
  public Future<RecordMetadata> send(String topicName, String key, Object message) throws Exception {
    if (firstSend) {
      // logger.warn("Sending message w/ TxnId: {}", this.txnId);
      firstSend = false;
    }
    return send(new ProducerRecord<>(topicName, key, message));
  }

}
