
package com.demo.output.mysql;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.stereotype.Service;

import com.demo.events.BaseConsumer;
import com.demo.messages.Record;
import com.demo.messages.Topics;
import com.demo.output.OutputTask;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

@Service
public class MySQLOutputTask extends OutputTask {

  private Logger logger = LoggerFactory.getLogger(MySQLOutputTask.class);

  private KafkaConsumer<String, Record> kafkaConsumer;

  private MySQLOuputEventRequest message;

  private JsonArray schema;

  private MySQLOutputSettings settings;

  public MySQLOutputTask() {

  }

  /**
   * This function will start a single worker thread per topic.
   * After creating the consumer object, we subscribed to a list of Kafka topics, in the constructor.
   * For this example, the list consists of only one topic. But you can give it a try with multiple topics.
   */
  public void execute(MySQLOuputEventRequest eventMessage, String groupId) {

    try {
      kafkaConsumer = new KafkaConsumer<String, Record>(BaseConsumer.getProperties(Topics.MYSQL_GRP_ID));
      message = eventMessage;

      settings = eventMessage.getSettings();
      // Properties consumerProperties, JsonArray schema
      this.schema = getSchema(settings.getObjectName());
      /*
       * We will start an infinite while loop, inside which we'll be listening to
       * new messages in each topic that we've subscribed to.MySQLOuputEventRequest
       */

      //kafkaConsumer.poll(0);
      initConsumer(kafkaConsumer, message);

      DataSource ds = getDataSource(message);
      while (true) {
        ConsumerRecords<String, Record> records = kafkaConsumer.poll(Duration.ofSeconds(60));

        if (records.count() == 0) {
          break;
        }
        List<Record> dataRecs = new ArrayList<Record>();
        ConsumerRecord<String, Record> lastRecord = null;
        for (ConsumerRecord<String, Record> record : records) {
          Record message = record.value();
          logger.info("Received message: " + message);
          try {
            dataRecs.add(message);
            lastRecord = record;
          } catch (Exception e) {
            logger.error(e.getMessage());
          }

        }
        if (lastRecord != null) {
          Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
          commitMessage.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()),
              new OffsetAndMetadata(lastRecord.offset() + 1));
          kafkaConsumer.commitSync(commitMessage);
        }
        if (dataRecs.size() > 0) {
          postData(ds, dataRecs);
        }

        logger.info("Posted data successfully!");
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }

  }

  public DataSource getDataSource(MySQLOuputEventRequest message) {
    DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
    dataSourceBuilder.driverClassName("com.mysql.cj.jdbc.Driver");
    dataSourceBuilder.url(settings.getBaseUrl());
    dataSourceBuilder.username(settings.getUserName());
    dataSourceBuilder.password(settings.getPassword());
    return dataSourceBuilder.build();
  }

  public void postData(DataSource ds, List<Record> dataRecs) {

    Connection con = null;
    PreparedStatement ps = null;
    String query = getInsertQuery();
    try {
      con = ds.getConnection();
      ps = con.prepareStatement(query);

      long start = System.currentTimeMillis();
      for (int i = 0; i < dataRecs.size(); i++) {
        Record r = dataRecs.get(i);
        int colIndex = 1;
        for (JsonElement column : schema) {
          JsonObject obj = column.getAsJsonObject();
          String key = obj.get("fieldName").getAsString();
          Object val = r.get(key);
          if (val instanceof Double) {
            ps.setDouble(colIndex, (Double) val);
          } else if (val instanceof Integer) {
            ps.setInt(colIndex, (Integer) val);
          } else if (val instanceof BigDecimal) {
            ps.setBigDecimal(colIndex, (BigDecimal) val);
          } else {
            ps.setString(colIndex, ((String) val));
          }
          colIndex++;
        }
        ps.addBatch();

        if (i % 1000 == 0) ps.executeBatch();
      }
      ps.executeBatch();

      logger.info("Time Taken - {}" + (System.currentTimeMillis() - start));

    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (ps != null) {
          ps.close();
        }
        con.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private String getInsertQuery() {
    StringBuilder sb = new StringBuilder();
    StringBuilder placeHolder = new StringBuilder();
    sb.append("insert into ")
        .append(settings.getObjectName())
        .append("(");
    schema.forEach(element -> {
      logger.info(element.getAsJsonObject().toString());
      JsonObject obj = element.getAsJsonObject();
      sb.append(obj.get("fieldName").getAsString()).append(",");
      placeHolder.append("?,");
    });

    sb.deleteCharAt(sb.length() - 1);
    placeHolder.deleteCharAt(placeHolder.length() - 1);
    sb.append(") values (");
    sb.append(placeHolder.toString());
    sb.append(")");

    return sb.toString();
  }

}
