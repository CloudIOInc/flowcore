
package com.demo.input;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.demo.events.TaskEventConsumer.EventType;
import com.demo.messages.Message;
import com.demo.messages.ResponseMessage;
import com.demo.messages.TaskEvent;
import com.demo.messages.Topics;

@Service
public class OracleInputTask implements InputTask {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  TaskEvent message;

  @Value(value = "${topic.partition}")
  private int topicPartition;

  @Value(value = "${topic.replica}")
  private int topicReplica;

  @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
  private String bootstrapAddress;

  @Value(value = "${spring.kafka.consumer.group-id}")
  private String groupId;

  private Logger logger = Logger.getLogger("OracleSubTask");

  @Override
  public String getSQL() {
    return "Select COUNT(1) ROW_COUNT from " + message.getInputSettings().getObjectName();
  }

  @Override
  public void execute(TaskEvent message) {
    this.message = message;
    long stime = new Date().getTime();
    try {
      String topicName = "oralce_input_task_topic";
      IO.createTopic(IO.getAdminClient(), topicName, topicPartition, false);
      //create data topic
      String dtTopic = message.getDataTopic();
      IO.createTopic(IO.getAdminClient(), dtTopic, topicPartition, false);
      executeInternal(message, topicName);
      //  postEvent(message, totalRows);
      DataSource ds = getDataSource(message);
      OracleInputTaskConsumer inputConsumer = new OracleInputTaskConsumer(ds);
      IO.executeOnKafkaScheduler(inputConsumer);
    } catch (Exception e) {
      logger.severe("Failed to execute OracleInputTask" + e.getMessage());
    }
    long etime = new Date().getTime();
    logger.info("total time to load data in Kafka topic is " + (etime - stime) / 1000 + " seconds");
  }

  private void executeInternal(TaskEvent message, String topic) {
    DataSource ds = getDataSource(message);
    List<OracleBatchInfo> batchInfoList = getBatchDetails(ds);
    for (OracleBatchInfo batchInfo : batchInfoList) {
      kafkaTemplate.send(topic, batchInfo.toString());
    }

    postEvent(message);
  }

  private void postEvent(Message message) {
    ResponseMessage response = new ResponseMessage();
    response.setEventType(EventType.Response.name());
    // response.setInputCount(totalRows);
    response.setWfFlowId(message.getWfFlowId());
    response.setWfFlowInstanceId(message.getWfFlowInstanceId());

    kafkaTemplate.send(Topics.EVENT_TOPIC, response.toString());
  }

  private List<OracleBatchInfo> getBatchDetails(DataSource ds) {
    long count = 0;
    List<OracleBatchInfo> batchInfo = new ArrayList<OracleBatchInfo>();

    Statement stmt = null;
    ResultSet rs = null;
    Connection conn = null;
    try {
      conn = ds.getConnection();
      String objectName = message.getInputSettings().getObjectName();
      String sql = getSQL();
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);

      while (rs.next()) {
        count = rs.getLong("ROW_COUNT");
      }
      long partition = message.getInputSettings().getPartition();
      if (count > partition) {
        long remainder = count % partition;

        int totalBatches = (int) (count / partition + (remainder == 0 ? 0 : 1));

        int lastIndex = totalBatches - 1;

        for (int i = lastIndex; i >= 0; i--) {
          long offset = i * partition; // 200 *10 = 2000
          OracleBatchInfo batch = new OracleBatchInfo();
          batch.setOffset(offset);
          batch.setFetchCount((offset + partition)); // 2000 + 200 = 2200
          batch.setObjectName(objectName);
          batchInfo.add(batch);

        }
      } else {
        OracleBatchInfo batch = new OracleBatchInfo();
        batch.setOffset(0);
        batch.setFetchCount(count);
        batch.setObjectName(objectName);
        batchInfo.add(batch);
      }
    } catch (Exception e) {
      logger.severe("Failed to getbatch details" + e.getMessage());
    } finally {
      try {
        if (stmt != null)
          stmt.close();
        if (rs != null)
          rs.close();
        if (conn != null)
          conn.close();
      } catch (SQLException e) {
        logger.severe("Failed to close resource " + e.getMessage());
      }
    }

    return batchInfo;
  }

  public DataSource getDataSource(TaskEvent message) {
    DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
    dataSourceBuilder.driverClassName("oracle.jdbc.driver.OracleDriver");
    dataSourceBuilder.url(message.getInputSettings().getBaseUrl());
    dataSourceBuilder.username(message.getInputSettings().getUserName());
    dataSourceBuilder.password(message.getInputSettings().getPassword());
    return dataSourceBuilder.build();
  }

}
