
package io.cloudio.task;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.DataConsumer;

public abstract class OutputTask<K, V> extends BaseTask<K, V> {

  protected DataConsumer dataConsumer;

  private static Logger logger = LogManager.getLogger(OutputTask.class);

  OutputTask(String taskCode) {
    super(taskCode);
  }

  public void handleEvent() throws Exception {
    subscribeData();
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
