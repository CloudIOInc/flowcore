
package io.cloudio.task;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.TaskConsumer;
import io.cloudio.exceptions.CloudIOException;
import io.cloudio.messages.OracleSettings;
import io.cloudio.messages.OracleTaskRequest;
import io.cloudio.messages.TaskRequest;
import io.cloudio.producer.Producer;
import io.cloudio.util.GsonUtil;
import io.cloudio.util.Util;

public abstract class OracleInputTask extends InputTask<TaskRequest<OracleSettings>, Data> {
  private static Logger logger = LogManager.getLogger(OracleInputTask.class);
  private static final String ORACLE_SUBTASK_STATUS = "oracle_subtask_status";
  private static final String ORACLE_SUB_TASKS = "oracle_sub_tasks";
  private TaskConsumer subTaskConsumer;
  private TaskConsumer subTaskStatuseventConsumer;
  static ExecutorService executorService = Executors.newFixedThreadPool(8);
  private Integer totalSubtask;
  private AtomicInteger subtask_recv_count = new AtomicInteger(0);

  public void createTopics() throws Exception {
    createTopic(ORACLE_SUB_TASKS, bootStrapServer, partitions);
    createTopic(ORACLE_SUBTASK_STATUS, bootStrapServer, partitions);

  }

  protected OracleInputTask(String taskCode) throws Exception {
    super(taskCode);
  }

  public abstract Integer getCountSql(OracleSettings settings, String tableName) throws Exception;

  public abstract List<Data> queryData(OracleTaskRequest<OracleSettings> event) throws Exception;

  public void start(String bootStrapServer, int partition) throws Exception {
    super.start(bootStrapServer, partition);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          executorService.shutdown();
          executorService.awaitTermination(1, TimeUnit.MINUTES);
          Thread.sleep(1 * 60 * 1000);
          Util.closeQuietly(subTaskStatuseventConsumer);
          Util.closeQuietly(subTaskConsumer);
          Util.closeQuietly(taskConsumer);
        } catch (Exception e) {
          //ignore
        }
      }
    });
  }

  private void createSubTaskStatusConsumer() {
    if (subTaskStatuseventConsumer == null) {
      subTaskStatuseventConsumer = new TaskConsumer("oracle_sub_task_status-" + UUID.randomUUID(),
          Collections.singleton(ORACLE_SUBTASK_STATUS));
      subTaskStatuseventConsumer.createConsumer();
      subTaskStatuseventConsumer.subscribe();
      executorService.execute(() -> subscribeSubTaskStatusEvent());
    } else {
      subTaskStatuseventConsumer.start();
    }
  }

  public void createSubTaskConsumer() throws Exception {
    if (subTaskConsumer == null) {
      subTaskConsumer = new TaskConsumer("oracle_sub_task",
          Collections.singleton(ORACLE_SUB_TASKS));
      subTaskConsumer.createConsumer();
      subTaskConsumer.subscribe();
      executorService.execute(() -> subscribeSubTaskEvent());
    } else {
      subTaskConsumer.start();
    }
  }

  private void subscribeSubTaskStatusEvent() {

    Throwable ex = null;
    try {
      while (true) {
        if (isLeader.get() && subTaskStatuseventConsumer.canRun()) {
          ConsumerRecords<String, String> events = subTaskStatuseventConsumer.poll();
          if (events != null && events.count() > 0) {
            for (TopicPartition partition : events.partitions()) {
              List<ConsumerRecord<String, String>> partitionRecords = events.records(partition);
              if (partitionRecords.size() == 0) {
                continue;
              }
              if (logger.isInfoEnabled()) {
                logger.info("Got {} subtask status events between {} & {} in {}", partitionRecords.size(),
                    partitionRecords.get(0).offset(),
                    partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
              }
              subtask_recv_count.addAndGet(partitionRecords.size());
              logger.info("Total subtask -{}, recv count -{}", totalSubtask, subtask_recv_count.get());
              if (totalSubtask == subtask_recv_count.get()) {
                //send Task End Response
                //send EndMessage to each of the partitions in Data Topic

                sendTaskEndResponse(taskRequest);
                sendEndMessage();
                subTaskStatuseventConsumer.close();
                totalSubtask = 0;
                subtask_recv_count.set(0);
                isLeader.compareAndSet(true, false);
              }
              ex = commitAndHandleErrors(subTaskStatuseventConsumer, partition, partitionRecords);
            }
            if (ex != null) {
              throw ex;
            }
          }
        }
        // subTaskConsumer.close();
      }
    } catch (WakeupException | InterruptException e) {
      // logger.warn("{} wokeup/interrupted...", getName());
    } catch (Throwable e) {
      logger.catching(e);
    }
    logger.debug("Stopped subtask status consumer for {} task " + taskCode);

  }

  private void subscribeSubTaskEvent() {
    Throwable ex = null;
    try {
      while (true) {
        if (subTaskConsumer.canRun()) {
          ConsumerRecords<String, String> events = subTaskConsumer.poll();
          if (events != null && events.count() > 0) {
            for (TopicPartition partition : events.partitions()) {
              List<ConsumerRecord<String, String>> partitionRecords = events.records(partition);
              if (partitionRecords.size() == 0) {
                continue;
              }
              if (logger.isInfoEnabled()) {
                logger.info("Got {} sub task events between {} & {} in {}", partitionRecords.size(),
                    partitionRecords.get(0).offset(),
                    partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
              }

              for (ConsumerRecord<String, String> record : partitionRecords) {
                String eventSting = record.value();
                if (eventSting == null) {
                  continue;
                }
                OracleTaskRequest<OracleSettings> eventObj = getTaskRequest(eventSting);
                List<Data> data = queryData(eventObj);
                post(data, eventObj);
                postStatusEvent(eventObj);
              }

              ex = commitAndHandleErrors(subTaskConsumer, partition, partitionRecords);
            }
            if (ex != null) {
              throw ex;
            }
          }
        }
        // subTaskConsumer.close();
      }
    } catch (WakeupException | InterruptException e) {
      // logger.warn("{} wokeup/interrupted...", getName());
    } catch (Throwable e) {
      logger.catching(e);
    }
    logger.debug("Stopped subtask consumer for {} task " + taskCode);

  }

  private void postStatusEvent(OracleTaskRequest<OracleSettings> eventObj) throws Exception {
    try (Producer producer = Producer.get()) {
      producer.beginTransaction();
      producer.send(ORACLE_SUBTASK_STATUS, eventObj);
      producer.commitTransaction();
    }
  }

  @Override
  public void handleData(TaskRequest<OracleSettings> event) throws Exception {
    OracleSettings s = event.getSettings();
    createSubTasks(s);
    sendTaskStartResponse(taskRequest, groupId);
    if (isLeader.get()) {
      createSubTaskStatusConsumer();
    }
  }

  private void createSubTasks(OracleSettings settings) {
    try {
      String tableName = settings.getTableName();
      Integer partitionSize = settings.getPartitionSize();
      Integer rowCount = getCountSql(settings, tableName);
      Integer subTasks = getTasks(rowCount, partitionSize);
      this.totalSubtask = subTasks;
      produceSubTaskMessages(subTasks, settings, ORACLE_SUB_TASKS);
    } catch (Exception e) {
      logger.error("Error while creating sub tasks - ", e.getMessage());
      throw new CloudIOException(e);
    }
  }

  private void produceSubTaskMessages(int subTasks, OracleSettings settings, String topic) throws Exception {
    try (Producer producer = Producer.get()) {
      producer.beginTransaction();
      for (int i = 1; i <= subTasks; i++) {
        OracleTaskRequest<OracleSettings> event = getTaskRequest(subTasks, i);
        event.setSettings(settings);
        producer.send(topic, "key-" + i, event);
      }
      producer.commitTransaction();
    }
  }

  private OracleTaskRequest<OracleSettings> getTaskRequest(int subTasks, int i) {
    OracleTaskRequest<OracleSettings> e = new OracleTaskRequest<OracleSettings>();
    e.setPageNo(i);
    e.setOffset((i - 1) * taskRequest.getSettings().getPartitionSize());
    e.setLimit(i * taskRequest.getSettings().getPartitionSize());
    e.setTotalPages(subTasks);
    e.setToTopic(taskRequest.getToTopic());
    e.setSettings(taskRequest.getSettings());
    e.setExecutionId(taskRequest.getExecutionId());
    e.setInputParams(taskRequest.getInputParams());
    e.setStartDate(taskRequest.getStartDate());
    e.setWfInstUid(taskRequest.getWfInstUid());
    e.setNodeUid(taskRequest.getNodeUid());
    e.setWfUid(taskRequest.getWfUid());
    return e;
  }

  private int getTasks(Integer toatl, Integer fetchSize) {
    if (toatl % fetchSize == 0) {
      return (toatl / fetchSize);
    } else {
      return (toatl / fetchSize) + 1;
    }
  }

  @Override
  protected OracleTaskRequest getTaskRequest(String eventJson) {
    return GsonUtil.getDBSettingsEvent(eventJson);
  }

  protected Data populateData(ResultSet rs, OracleSettings settings) throws Exception {
    Data d = new Data();
    List<HashMap<String, Object>> schema = getSchema(settings.getTableName());
    for (int i = 0; i < schema.size(); i++) {
      Map<String, Object> field = schema.get(i);
      String fieldName = (String) field.get("fieldName");
      Object obj = rs.getObject(fieldName);
      d.put(fieldName, obj);
    }
    return d;
  }

}
