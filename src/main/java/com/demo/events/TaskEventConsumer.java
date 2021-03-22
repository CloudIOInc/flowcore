
package com.demo.events;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.demo.input.oracle.OracleInputEventRequest;
import com.demo.input.oracle.OracleInputEventResponse;
import com.demo.input.oracle.OracleInputTask;
import com.demo.messages.Context;
import com.demo.messages.EventRequest;
import com.demo.messages.OutputMessage;
import com.demo.messages.Settings;
import com.demo.output.transform.UpperCaseTransformEventRequest;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

@Service
public class TaskEventConsumer {

  public enum TaskType {
    Input, Output, Tansform, Response
  }

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
    EventRequest<Settings, Context> request = getEventRequest(message);

    TaskType eventType = TaskType.valueOf(request.getTaskType());
    switch (eventType) {
    case Input:
      handleInputMessage(request);
      break;
    case Output:
      handleOutputMessage(request);
      break;
    default:
      //ignore for now
    }
  }

  private void handleOutputMessage(EventRequest request) throws Exception {
    //    Output input = Output.valueOf(messageObj.getOutputSettings().getType());
    //    JsonArray schema = getSchema(messageObj.getOutputSettings().getObjectName());
    //    switch (input) {
    //    case MySQL:
    //      MySQLOutputTask task = new MySQLOutputTask(messageObj, getConsumberProps(), schema);
    //      task.execute();
    //      break;
    //    default:
    //      throw new Exception("Invalid output type[" + input.name() + "]");
    //    }
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

  private void handleInputMessage(EventRequest request) throws Exception {
    Input input = Input.valueOf(request.getSettings().getType());
    switch (input) {
    case Oracle:
      task.execute((OracleInputEventRequest) request);
      break;
    default:
      throw new Exception("Invalid input type[" + input.name() + "]");
    }
  }

  static Gson gson = new Gson();

  public static <OracleInputConext> EventRequest<Settings, Context> getEventRequest(String message) {
    JsonElement input = JsonParser.parseString(message);
    JsonObject inputObj = input.getAsJsonObject();
    Type objType = new TypeToken<EventRequest<Settings, Context>>() {
    }.getType();
    if (TaskType.Input.name().equals(inputObj.get("taskType").getAsString())) {
      if (TaskType.Tansform.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {

      } else if (Input.Oracle.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {
        objType = new TypeToken<UpperCaseTransformEventRequest>() {
        }.getType();
      }
    } else if (TaskType.Output.name().equals(inputObj.get("taskType").getAsString())) {
      objType = new TypeToken<OutputMessage>() {
      }.getType();
    } else if (TaskType.Response.name().equals(inputObj.get("taskType").getAsString())) {
      if (TaskType.Tansform.name().equals(inputObj.get("settings").getAsString())) {

      } else if (Input.Oracle.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {

        objType = new TypeToken<OracleInputEventResponse>() {
        }.getType();
      }
    }
    return gson.fromJson(message, objType);
  }

}
