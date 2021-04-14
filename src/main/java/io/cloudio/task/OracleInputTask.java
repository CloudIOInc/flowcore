
package io.cloudio.task;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.consumer.EventConsumer;
import io.cloudio.exceptions.CloudIOException;
import io.cloudio.messages.DBEvent;
import io.cloudio.messages.DBSettings;
import io.cloudio.messages.Settings;
import io.cloudio.producer.Producer;
import io.cloudio.util.GsonUtil;
import oracle.jdbc.driver.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

public abstract class OracleInputTask extends InputTask<Event<DBSettings>, Data> {
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
    // TODO Auto-generated method stub

  }

  private void subscribeSubTaskEvent(String oracleSubTasks) {
    // TODO Auto-generated method stub

  }

  @Override
  public void handleData(Event<DBSettings> event) {
    isLeader = true;
    DBSettings s = event.getSettings();
    createSubTasks(s);
    subscribeParallelEvents();

  }

  private void createSubTasks(DBSettings settings) {
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

  private void createSubTaskEvents(int subTasks, DBSettings settings) throws Exception {
    Producer producer = Producer.get();
    producer.beginTransaction();
    for (int i = 1; i <= subTasks; i++) {
      DBEvent event = getDBEvent(subTasks, i);
      producer.send(ORACLE_SUB_TASKS, event);
    }
    producer.commitTransaction();

  }

  private DBEvent getDBEvent(int subTasks, int i) {
    DBEvent e = new DBEvent();
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

  private Connection getConnection(DBSettings settings) throws Exception {
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
  protected Event<? extends Settings> getEvent(String eventJson) {
    return GsonUtil.getDBSettingsEvent(eventJson);
  }

}
