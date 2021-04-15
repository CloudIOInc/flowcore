
package io.cloudio.task;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.EventConsumer;
import io.cloudio.exceptions.CloudIOException;
import io.cloudio.messages.OracleEvent;
import io.cloudio.messages.OracleSettings;
import io.cloudio.producer.Producer;
import io.cloudio.util.GsonUtil;
import oracle.jdbc.driver.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

public abstract class OracleInputTask extends InputTask<Event<OracleSettings>, Data> {
  private static final String ORACLE_SUBTASK_STATUS = "oracle_subtask_status";
  private static final String ORACLE_SUB_TASKS = "oracle_sub_tasks";
  private EventConsumer subTaskConsumer;
  private EventConsumer subTaskStatuseventConsumer;
  private static Logger logger = LogManager.getLogger(OracleInputTask.class);

  private boolean isLeader = false;

  OracleInputTask(String taskCode) {
    super(taskCode);

  }

  public abstract String getCountSql(String tableName);

  public abstract List<Data> queryData(OracleEvent<OracleSettings> event);

  public void start() {
    super.start();

    subscribeParallelEvents();

  }

  private void subscribeParallelEvents() {
    subTaskConsumer = new EventConsumer(groupId, Collections.singleton(ORACLE_SUB_TASKS));
    subTaskConsumer.createConsumer();
    subTaskConsumer.subscribe();
    subscribeSubTaskEvent(ORACLE_SUB_TASKS);

    if (isLeader) {
      subTaskStatuseventConsumer = new EventConsumer(groupId, Collections.singleton(ORACLE_SUBTASK_STATUS));
      subTaskStatuseventConsumer.createConsumer();
      subTaskStatuseventConsumer.subscribe();
      subscribeSubTaskStatusEvent(ORACLE_SUBTASK_STATUS);
    }
  }

  private void subscribeSubTaskStatusEvent(String oracleSubtaskStatus) {

  }

  private void subscribeSubTaskEvent(String oracleSubTasks) {

    Throwable ex = null;

    try {
      ConsumerRecords<String, String> events = subTaskConsumer.poll();
      if (events != null && events.count() > 0) {
        for (TopicPartition partition : events.partitions()) {
          List<ConsumerRecord<String, String>> partitionRecords = events.records(partition);
          if (partitionRecords.size() == 0) {
            continue;
          }
          if (logger.isInfoEnabled()) {
            logger.info("Got {} events between {} & {} in {}", partitionRecords.size(),
                partitionRecords.get(0).offset(),
                partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
          }

          for (ConsumerRecord<String, String> record : partitionRecords) {
            String eventSting = record.value();
            if (eventSting == null) {
              continue;
            }
            OracleEvent<OracleSettings> eventObj = getEvent(eventSting);
            List<Data> data = queryData(eventObj);
            post(data);
          }

          try {

            if (partitionRecords.size() > 0) {
              long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
              subTaskConsumer.commitSync(
                  Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
          } catch (WakeupException | InterruptException e) {
            throw e;
          } catch (Throwable e) {
            logger.catching(e);
            if (ex == null) {
              ex = e;
            }
            try {
              logger.error("Seeking back {} events between {} & {} in {}", partitionRecords.size(),
                  partitionRecords.get(0).offset(),
                  partitionRecords.get(partitionRecords.size() - 1).offset(),
                  partition.toString());

              subTaskConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
            } catch (Throwable e1) {
              subTaskConsumer.restartBeforeNextPoll();
            }
          }
        }
        if (ex != null) {
          throw ex;
        }
      }
    } catch (Throwable e) {
      logger.catching(e);
    }
    logger.debug("Stopped event consumer for {} task " + taskCode);

  }

  @Override
  public void handleData(Event<OracleSettings> event) {
    isLeader = true;
    OracleSettings s = event.getSettings();
    createSubTasks(s);
    subscribeParallelEvents();

  }

  private void createSubTasks(OracleSettings settings) {
    try {
      String tableName = settings.getTableName();
      int fetchSize = settings.getFetchSize();
      String cntSql = getCountSql(tableName);
      try (Connection con = getConnection(settings)) {
        Statement stmt = con.createStatement();
        stmt.closeOnCompletion();
        try (ResultSet rs = stmt.executeQuery(cntSql)) {
          int rowCount = rs.getInt("ROW_COUNT");
          int subTasks = getTasks(rowCount, fetchSize);
          createSubTaskEvents(subTasks, settings);
        }

      }
    } catch (Exception e) {
      logger.error("Error while creating sub tasks - ", e.getMessage());
      throw new CloudIOException(e);
    }
  }

  private void createSubTaskEvents(int subTasks, OracleSettings settings) throws Exception {
    Producer producer = Producer.get();
    producer.beginTransaction();
    for (int i = 1; i <= subTasks; i++) {
      OracleEvent<OracleSettings> event = getDBEvent(subTasks, i);
      producer.send(ORACLE_SUB_TASKS, event);
    }
    producer.commitTransaction();

  }

  private OracleEvent<OracleSettings> getDBEvent(int subTasks, int i) {
    OracleEvent<OracleSettings> e = new OracleEvent<OracleSettings>();
    e.setPageNo(1);
    e.setTotalPages(subTasks);
    e.setFromTopic(event.getToTopic());
    e.setSettings(event.getSettings());
    e.setWfUid(event.getWfUid());
    return e;
  }

  private int getTasks(int toatl, int fetchSize) {
    if (toatl % fetchSize == 0) {
      return (toatl / fetchSize);
    } else {
      return (toatl / fetchSize) + 1;
    }
  }

  private Connection getConnection(OracleSettings settings) throws Exception {
    Properties info = new Properties();
    info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, settings.getUserName());
    info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, settings.getPassword());
    info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");

    OracleDataSource ods = new OracleDataSource();
    ods.setURL(settings.getJdbcUrl());
    ods.setConnectionProperties(info);
    return ods.getConnection();

  }

  @Override
  protected OracleEvent<OracleSettings> getEvent(String eventJson) {
    return GsonUtil.getDBSettingsEvent(eventJson);
  }

}
