
package com.demo.input;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.demo.messages.OracleInputEventRequest;
import com.demo.messages.OracleInputEventResponse;
import com.demo.messages.Topics;

@Service
public class OracleInputTask {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  OracleInputEventRequest request;

  @Value(value = "${topic.partition}")
  private int topicPartition;

  @Value(value = "${topic.replica}")
  private int topicReplica;

  @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
  private String bootstrapAddress;

  @Value(value = "${spring.kafka.consumer.group-id}")
  private String groupId;

  private Logger logger = Logger.getLogger("OracleSubTask");

  public String getSQL() {
    return "Select COUNT(1) ROW_COUNT from " + request.getSettings().getTableName();
  }

  public void execute(OracleInputEventRequest request) {
    this.request = request;
    long stime = new Date().getTime();
    try {
      String topicName = "oralce_input_task_topic";
      KafkaUtil.createTopic(KafkaUtil.getAdminClient(), topicName, topicPartition, false);
      //create data topic
      String toTopic = request.getToTopic();
      KafkaUtil.createTopic(KafkaUtil.getAdminClient(), toTopic, topicPartition, false);
      executeInternal(request, topicName);
      //  postEvent(message, totalRows);
      DataSource ds = getDataSource(request);
      OracleInputTaskConsumer inputConsumer = new OracleInputTaskConsumer(ds);
      KafkaUtil.executeOnKafkaScheduler(inputConsumer);
      postEvent(request);
    } catch (Exception e) {
      logger.severe("Failed to execute OracleInputTask" + e.getMessage());
    }
    long etime = new Date().getTime();
    logger.info("total time to load data in Kafka topic is " + (etime - stime) / 1000 + " seconds");
  }

  private void executeInternal(OracleInputEventRequest request, String topic) {
    DataSource ds = getDataSource(request);
    List<OracleBatchInfo> batchInfoList = getBatchDetails(ds);
    for (OracleBatchInfo batchInfo : batchInfoList) {
      kafkaTemplate.send(topic, batchInfo.toString());
    }
  }

  private void postEvent(OracleInputEventRequest request) {
    OracleInputEventResponse response = new OracleInputEventResponse();
    response.setEndOffset(null);
    response.setStartOffset(null);
    response.setStatus("RUNNING");

    // response.setInputCount(totalRows);
    response.setWfFlowId(request.getWfFlowId());
    response.setWfFlowInstanceId(request.getWfFlowInstanceId());
    Map<Integer, Integer> endOffsets = KafkaUtil.getEndOffsets(request.getToTopic(), groupId, topicPartition);
    response.setEndOffset(endOffsets);
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
      String objectName = request.getSettings().getTableName();
      String sql = getSQL();
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);

      while (rs.next()) {
        count = rs.getLong("ROW_COUNT");
      }
      long partition = request.getSettings().getPartition();
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

  public DataSource getDataSource(OracleInputEventRequest message) {
    DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
    dataSourceBuilder.driverClassName("oracle.jdbc.driver.OracleDriver");
    dataSourceBuilder.url(message.getSettings().getJdbcUrl());
    dataSourceBuilder.username(message.getSettings().getUserName());
    dataSourceBuilder.password(message.getSettings().getPassword());
    return dataSourceBuilder.build();
  }

}
