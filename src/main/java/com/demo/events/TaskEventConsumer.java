
package com.demo.events;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.demo.input.oracle.OracleInputEventRequest;
import com.demo.input.oracle.OracleInputTask;
import com.demo.messages.Event;
import com.demo.messages.EventRequest;
import com.demo.messages.Record;
import com.demo.output.mysql.MySQLOuputEventRequest;
import com.demo.output.mysql.MySQLOutputTask;
import com.demo.output.transform.UpperCaseTransformEventRequest;
import com.demo.output.transform.UpperCaseTransformOutputTask;
import com.google.gson.Gson;

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

  @Autowired
  MySQLOutputTask mySQLTask;

  @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
  private String bootstrapAddress;
  @Value(value = "${spring.kafka.consumer.group-id}")
  private String groupId;

  @KafkaListener(topics = "wf_instance_task_events", groupId = "task_events")
  public void consume(Event request) throws Exception {
    //split events
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

  private void handleTransformMessage(Event request) throws Exception {
    TaskSubType taskSubtype = TaskSubType.valueOf(((EventRequest) request).getSettings().getType());
    switch (taskSubtype) {
    case UpperCase:
      UpperCaseTransformOutputTask task = new UpperCaseTransformOutputTask(groupId,
          (UpperCaseTransformEventRequest) request);

      task.execute();
      break;
    default:
      throw new Exception("Invalid transform task type [" + taskSubtype.name() + "]");
    }
  }

  private void handleOutputMessage(Event request) throws Exception {
    TaskSubType taskSubtype = TaskSubType.valueOf(((EventRequest) request).getSettings().getType());
    switch (taskSubtype) {
    case MySQL:
      //MySQLOutputTask task = new MySQLOutputTask((MySQLOuputEventRequest) request, groupId);
      mySQLTask.execute((MySQLOuputEventRequest) request, groupId);
      break;
    default:
      throw new Exception("Invalid output task type [" + taskSubtype.name() + "]");
    }
  }

  private void handleInputMessage(Event request) throws Exception {
    TaskSubType input = TaskSubType.valueOf(((EventRequest) request).getSettings().getType());
    switch (input) {
    case Oracle:
      oracleInputTask.execute((OracleInputEventRequest) request);
      break;
    default:
      throw new Exception("Invalid input task type [" + input.name() + "]");
    }
  }

  static Gson gson = new Gson();

  //  public static EventRequest<Settings, Context> getEventRequest(Record message) {
  //    //  JsonElement input = JsonParser.parseString(message);
  //    //  JsonObject inputObj = input.getAsJsonObject();
  //
  //    Type objType = new TypeToken<EventRequest<Settings, Context>>() {
  //    }.getType();
  //    objType = getEventType(message, objType);
  //    return gson.fromJson(message.toJSON().getAsString(), objType);
  //  }

  private boolean isEventReponse(Record message) {
    String taskType = message.getAsString("taskType");
    if (TaskType.Response.name().equals(taskType)) {
      return true;
    }
    return false;
  }

}
