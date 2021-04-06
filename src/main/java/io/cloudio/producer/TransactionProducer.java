
package io.cloudio.producer;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface TransactionProducer<K, V> extends AutoCloseable {

  void abortTransactionQuietly();

  void abortTransaction();

  void beginTransaction();

  void commitTransaction();

  void commitTransactionQuietly();

  Future<RecordMetadata> send(String topicName, K key, V message) throws Exception;

}