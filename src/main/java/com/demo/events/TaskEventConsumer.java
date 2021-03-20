
package com.demo.events;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.demo.input.OracleInputTask;
import com.demo.messages.Message;
import com.demo.messages.OutputMessage;
import com.demo.messages.ResponseMessage;
import com.demo.messages.TaskEvent;
import com.demo.output.MySQLOutputTask;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

@Service
public class TaskEventConsumer {
  public enum Input {
    Oracle, MySQL, Salesforce
  }

  public enum Output {
    Oracle, MySQL, Salesforce
  }

  public enum EventType {
    Input, Output, Response
  }

  @Autowired
  OracleInputTask task;

  @Autowired
  private ResourceLoader resourceLoader;

  @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
  private String bootstrapAddress;
  @Value(value = "${spring.kafka.consumer.group-id}")
  private String groupId;

  @KafkaListener(topics = "wf_instance_task_events", groupId = "task_events")
  public void consume(String message) throws Exception {
    //split events
    Message messageObj = getEventRequest(message);

    EventType eventType = EventType.valueOf(messageObj.getEventType());
    switch (eventType) {
    case Input:
      handleInputMessage((TaskEvent) messageObj);
      break;
    case Output:
      handleOutputMessage((OutputMessage) messageObj);
      break;
    default:
      //ignore for now
    }
  }

  private void handleOutputMessage(OutputMessage messageObj) throws Exception {
    Output input = Output.valueOf(messageObj.getOutputSettings().getType());
    JsonArray schema = getSchema(messageObj.getOutputSettings().getObjectName());
    switch (input) {
    case MySQL:
      MySQLOutputTask task = new MySQLOutputTask(messageObj, getConsumberProps(), schema);
      task.execute();
      break;
    default:
      throw new Exception("Invalid output type[" + input.name() + "]");
    }
  }

  private JsonArray getSchema(String objectName) throws Exception {
    Resource resource = resourceLoader.getResource("classpath:spec.json");
    InputStream dbAsStream = resource.getInputStream();
    InputStreamReader rd = new InputStreamReader(dbAsStream);
    JsonElement el = JsonParser.parseReader(rd);
    JsonObject obj = el.getAsJsonObject();
    if (obj.get(objectName) != null) {
      return obj.get(objectName).getAsJsonArray();
    }
    throw new Exception("No schema found for - [" + objectName + "]");
  }

  private void handleInputMessage(TaskEvent messageObj) throws Exception {
    Input input = Input.valueOf(messageObj.getInputSettings().getType());
    switch (input) {
    case Oracle:
      task.execute(messageObj);

      break;
    default:
      throw new Exception("Invalid input type[" + input.name() + "]");
    }
  }

  static Gson gson = new Gson();

  public static Message getEventRequest(String message) {
    JsonElement input = JsonParser.parseString(message);
    JsonObject inputObj = input.getAsJsonObject();
    Type objType = new TypeToken<Message>() {
    }.getType();
    if (EventType.Input.name().equals(inputObj.get("eventType").getAsString())) {
      objType = new TypeToken<TaskEvent>() {
      }.getType();
    } else if (EventType.Output.name().equals(inputObj.get("eventType").getAsString())) {
      objType = new TypeToken<OutputMessage>() {
      }.getType();
    } else if (EventType.Response.name().equals(inputObj.get("eventType").getAsString())) {
      objType = new TypeToken<ResponseMessage>() {
      }.getType();
    }

    return gson.fromJson(message, objType);
  }

  private Properties getConsumberProps() {
    /*
     * Defining Kafka consumer properties.
     */
    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", bootstrapAddress);
    consumerProperties.put("group.id", "data_cons_grp");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put("auto.commit.interval.ms", "1000");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return consumerProperties;
  }
}
