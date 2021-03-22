
package com.demo.events;

import java.lang.reflect.Type;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.demo.input.oracle.OracleInputEventRequest;
import com.demo.input.oracle.OracleInputTask;
import com.demo.messages.Context;
import com.demo.messages.EventRequest;
import com.demo.messages.EventResponse;
import com.demo.messages.Settings;
import com.demo.output.mysql.MySQLOuputEventRequest;
import com.demo.output.mysql.MySQLOutputTask;
import com.demo.output.transform.UpperCaseTransformEventRequest;
import com.demo.output.transform.UpperCaseTransformOutputTask;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

@Service
public class TaskEventConsumer {

  public enum TaskType {
    Input, Output, Transform, Response
  }

  public enum TaskSubType {
    Oracle, MySQL, Salesforce, UpperCase, LowerCase, InitCaps
  }

  @Autowired
  OracleInputTask oracleInputTask;

  @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
  private String bootstrapAddress;
  @Value(value = "${spring.kafka.consumer.group-id}")
  private String groupId;

  @KafkaListener(topics = "wf_instance_task_events", groupId = "task_events")
  public void consume(String message) throws Exception {
    //split events

    if (getEventRespose(message) != null) {
      //skip all responses for now;
      return;
    }
    ;
    EventRequest<Settings, Context> request = getEventRequest(message);

    TaskType eventType = TaskType.valueOf(request.getTaskType());
    switch (eventType) {
    case Input:
      handleInputMessage(request);
      break;
    case Output:
      handleOutputMessage(request);
      break;
    case Transform:
      handleTransformMessage(request);
      break;
    default:
      //ignore for now
    }
  }

  private void handleTransformMessage(EventRequest request) throws Exception {
    TaskSubType taskSubtype = TaskSubType.valueOf(request.getSettings().getType());
    switch (taskSubtype) {
    case UpperCase:
      UpperCaseTransformOutputTask task = new UpperCaseTransformOutputTask(groupId,
          (UpperCaseTransformEventRequest) request);

      task.execute();
      break;
    default:
      throw new Exception("Invalid task type[" + taskSubtype.name() + "]");
    }
  }

  private void handleOutputMessage(EventRequest request) throws Exception {
    TaskSubType taskSubtype = TaskSubType.valueOf(request.getSettings().getType());
    switch (taskSubtype) {
    case MySQL:
      MySQLOutputTask task = new MySQLOutputTask((MySQLOuputEventRequest) request, groupId);
      task.execute();
      break;
    default:
      throw new Exception("Invalid task type[" + taskSubtype.name() + "]");
    }
  }

  private void handleInputMessage(EventRequest request) throws Exception {
    TaskSubType input = TaskSubType.valueOf(request.getSettings().getType());
    switch (input) {
    case Oracle:
      oracleInputTask.execute((OracleInputEventRequest) request);
      break;
    default:
      throw new Exception("Invalid input type[" + input.name() + "]");
    }
  }

  static Gson gson = new Gson();

  public static EventRequest<Settings, Context> getEventRequest(String message) {
    JsonElement input = JsonParser.parseString(message);
    JsonObject inputObj = input.getAsJsonObject();
    Type objType = new TypeToken<EventRequest<Settings, Context>>() {
    }.getType();
    if (TaskType.Input.name().equals(inputObj.get("taskType").getAsString())) {
      if (TaskSubType.Oracle.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {
        objType = new TypeToken<OracleInputEventRequest>() {
        }.getType();
      }
    } else if (TaskType.Transform.name().equals(inputObj.get("taskType").getAsString())) {
      if (TaskSubType.UpperCase.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {
        objType = new TypeToken<UpperCaseTransformEventRequest>() {
        }.getType();
      }
    } else if (TaskType.Output.name().equals(inputObj.get("taskType").getAsString())) {
      if (TaskSubType.MySQL.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {
        objType = new TypeToken<MySQLOuputEventRequest>() {
        }.getType();
      }
    }
    return gson.fromJson(message, objType);
  }

  public static EventResponse<Context> getEventRespose(String message) {
    JsonElement input = JsonParser.parseString(message);
    JsonObject inputObj = input.getAsJsonObject();
    Type objType = new TypeToken<EventRequest<Settings, Context>>() {
    }.getType();
    if (TaskType.Response.name().equals(inputObj.get("taskType").getAsString())) {

      objType = new TypeToken<EventResponse>() {
      }.getType();
      return gson.fromJson(message, objType);
    }
    return null;
  }

}
