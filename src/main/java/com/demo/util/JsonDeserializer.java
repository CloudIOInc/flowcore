/*
 * Copyright (c) 2014 - present CloudIO Inc.
 * 1248 Reamwood Ave, Sunnyvale, CA 94089
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * CloudIO Inc. ("Confidential Information").  You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with CloudIO.
 */

package com.demo.util;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.demo.events.TaskEventConsumer.TaskSubType;
import com.demo.events.TaskEventConsumer.TaskType;
import com.demo.input.oracle.OracleInputEventRequest;
import com.demo.messages.Context;
import com.demo.messages.EventResponse;
import com.demo.messages.Record;
import com.demo.output.mysql.MySQLOuputEventRequest;
import com.demo.output.transform.UpperCaseTransformEventRequest;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class JsonDeserializer<T> implements Deserializer<T> {
  private Class<T> deserializedClass;

  public JsonDeserializer(Class<T> deserializedClass) {
    this.deserializedClass = deserializedClass;
  }

  public JsonDeserializer() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> map, boolean isKey) {
    if (deserializedClass == null) {
      if (isKey) {
        deserializedClass = (Class<T>) map.get("key.deserializer");
      } else {
        if (map.get("value.deserializer") instanceof String) {
          try {
            deserializedClass = (Class<T>) Class.forName((String) map.get("value.deserializer"));
            return;
          } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
          }
        }
        ;
        deserializedClass = (Class<T>) map.get("value.deserializer");
      }
    }
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String str = new String(bytes, Util.UTF8);
    if (deserializedClass.toString().equals("io.cloudio.scale.kafka.flow.Message")) {
      return Util.getMessageDeserializer().fromJson(str, deserializedClass);
    }
    Type objType = getType(str);

    return Util.getDeserializer().fromJson(str, objType);
  }

  private static Type getType(String message) {
    Type objType = null;
    JsonElement input = JsonParser.parseString(message);
    JsonObject inputObj = input.getAsJsonObject();
    JsonElement taskType = inputObj.get("taskType");
    if (taskType != null) {
      objType = getEventType(objType, inputObj, taskType);
    } else {
      objType = new TypeToken<Record>() {
      }.getType();
    }
    return objType;
  }

  private static Type getEventType(Type objType, JsonObject inputObj, JsonElement taskType) {
    if (TaskType.Input.name().equals(taskType.getAsString())) {
      if (TaskSubType.Oracle.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {
        objType = new TypeToken<OracleInputEventRequest>() {
        }.getType();
      }
    } else if (TaskType.Transform.name().equals(taskType.getAsString())) {
      if (TaskSubType.UpperCase.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {
        objType = new TypeToken<UpperCaseTransformEventRequest>() {
        }.getType();
      }
    } else if (TaskType.Output.name().equals(taskType.getAsString())) {
      if (TaskSubType.MySQL.name().equals(inputObj.get("settings").getAsJsonObject().get("type").getAsString())) {
        objType = new TypeToken<MySQLOuputEventRequest>() {
        }.getType();
      }
    } else if (TaskType.Response.name().equals(taskType.getAsString())) {
      objType = new TypeToken<EventResponse<Context>>() {
      }.getType();
    }
    return objType;
  }

  @Override
  public void close() {

  }
}
