
package com.demo.output.mysql;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.demo.events.BaseConsumer;
import com.demo.output.OutputTask;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

@Service
public class MySQLOutputTask extends OutputTask {

  private Logger logger = LoggerFactory.getLogger(MySQLOutputTask.class);

  private KafkaConsumer<String, String> kafkaConsumer;

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
      kafkaConsumer = new KafkaConsumer<>(BaseConsumer.getProperties(groupId));
      message = eventMessage;
      
      settings = (MySQLOutputSettings)eventMessage.getSettings();
      // Properties consumerProperties, JsonArray schema
      this.schema = getSchema(settings.getObjectName());
      /*
       * We will start an infinite while loop, inside which we'll be listening to
       * new messages in each topic that we've subscribed to.MySQLOuputEventRequest
       */

      //kafkaConsumer.poll(0);
      initConsumer();
      DataSource ds = getDataSource(message);
      while (true) {

        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        if (records.count() == 0) {
          break;
        }
        List<JsonObject> dataRecs = new ArrayList<JsonObject>();
        for (ConsumerRecord<String, String> record : records) {
          String message = record.value();
          logger.info("Received message: " + message);
          try {
            JsonElement data = JsonParser.parseString(message);
            dataRecs.add(data.getAsJsonObject());
          } catch (Exception e) {
            logger.error(e.getMessage());
          }

          /*
          Once we finish processing a Kafka message, we have to commit the offset so that
          we don't end up consuming the same message endlessly. By default, the consumer object takes
          care of this. But to demonstrate how it can be done, we have turned this default behaviour off,
          instead, we're going to manually commit the offsets.
          The code for this is below. It's pretty much self explanatory.
           */
          {
            Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

            commitMessage.put(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));

            kafkaConsumer.commitSync(commitMessage);
          }
        }
        if(dataRecs.size() > 0) {
          postData(ds, dataRecs);
        }
        
        logger.info("Posted data successfully!");
      }
    }catch(Exception e) {
      logger.error(e.getMessage());
    }
    
  }

  private void initConsumer() {
    kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
    List<TopicPartition> partitions = new ArrayList<>();
    for (PartitionInfo partition : kafkaConsumer.partitionsFor(message.getToTopic()))
      partitions.add(new TopicPartition(message.getToTopic(), partition.partition()));
    kafkaConsumer.assign(partitions);
    Map<TopicPartition, Long> offset = kafkaConsumer.endOffsets(kafkaConsumer.assignment());
    Long endOffset = offset.get(kafkaConsumer.assignment());
    logger.info("End offset - {}", endOffset);
  }

  public DataSource getDataSource(MySQLOuputEventRequest message) {
    DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
    dataSourceBuilder.driverClassName("com.mysql.cj.jdbc.Driver");
    dataSourceBuilder.url(settings.getBaseUrl());
    dataSourceBuilder.username(settings.getUserName());
    dataSourceBuilder.password(settings.getPassword());
    return dataSourceBuilder.build();
  }

  public void postData(DataSource ds, List<JsonObject> dataRecs) {

    Connection con = null;
    PreparedStatement ps = null;
    String query = getInsertQuery();
    try {
      con = ds.getConnection();
      ps = con.prepareStatement(query);

      long start = System.currentTimeMillis();
      for (int i = 0; i < dataRecs.size(); i++) {
        JsonObject jsonObject = dataRecs.get(i);
        Set<String> keys = jsonObject.keySet();
        int colIndex = 1;
        for(String key : keys) {
          JsonPrimitive val = jsonObject.get(key).getAsJsonPrimitive();
          if(val.isNumber()) {
            ps.setInt(colIndex, val.getAsInt());
          }else {
            ps.setString(colIndex, val.getAsString());
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
    
    sb.deleteCharAt(sb.length()-1);
    placeHolder.deleteCharAt(placeHolder.length()-1);
    sb.append(") values (");
    sb.append(placeHolder.toString());
    sb.append(")");
    
    
    return sb.toString();
  }
  
  
}