
package io.cloudio.task;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.DataConsumer;
import io.cloudio.producer.Producer;
import io.cloudio.util.Util;

public abstract class OutputTask<K, V> extends BaseTask<K, V> {

  protected DataConsumer dataConsumer;

  private static Logger logger = LogManager.getLogger(OutputTask.class);
  Producer producer;

  OutputTask(String taskCode) {
    super(taskCode);
  }

  public void handleEvent() throws Exception {
    subscribeData();
  }

  public abstract void executeTask(Map<String, Object> inputParams, Map<String, Object> outputParams,
      Map<String, Object> inputState, List<Data> dataList) throws Exception;

  public void handleData(List<Data> dataList) throws Exception {
    boolean isError = false;
    Data endMessage = null;
    try {
      int lastIndex = dataList.size() - 1;
      endMessage = dataList.get(lastIndex);
      if (endMessage.isEnd()) {
        unsubscribeData();
        dataConsumer.complete();
        dataList.remove(lastIndex);
      }
      if (dataList.size() > 0) {
        producer = Producer.get();
        producer.beginTransaction();
        executeTask(taskRequest.getInputParams(), taskRequest.getOutputParams(),
            taskRequest.getInputState(), dataList);
        producer.commitTransaction();
      }
    } catch (Exception e) {
      logger.catching(e);
      dataConsumer.complete();
      producer.abortTransactionQuietly();
      isError = false;
      throw e;
    } finally {
      Util.closeQuietly(producer);
      if (endMessage != null && endMessage.isEnd()) {
        sendTaskEndResponse(taskRequest, isError);
      }
    }
  }

  private void subscribeData() throws Exception {
    String _id = "dt_consumer_output" + taskRequest.getFromTopic();
    List<Map<String, Integer>> offsets = taskRequest.getFromTopicStartOffsets();
    Integer partition = offsets.get(0).get("partition");
    Integer offset = offsets.get(0).get("offset");
    TopicPartition part = new TopicPartition(taskRequest.getFromTopic(), partition);
    dataConsumer = new DataConsumer(_id,
        Collections.singleton(taskRequest.getFromTopic()), (BaseTask<String, Data>) this, part, offset);
    dataConsumer.run();
    dataConsumer.await();
    logger.info("Subscribing data event for output task - {}", _id);
  }

  protected void unsubscribeData() {
    dataConsumer.close();
  }
}
