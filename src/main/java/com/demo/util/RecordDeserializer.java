
package com.demo.util;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.demo.messages.Record;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class RecordDeserializer implements JsonDeserializer<Object> {
  @Override
  public Object deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
    if (json.isJsonNull())
      return null;
    else if (json.isJsonPrimitive())
      return handlePrimitive(json.getAsJsonPrimitive());
    else if (json.isJsonArray())
      return handleArray(json.getAsJsonArray(), context);
    else
      return handleObject(json.getAsJsonObject(), context);
  }

  private Object handlePrimitive(JsonPrimitive json) {
    if (json.isBoolean())
      return json.getAsBoolean();
    else if (json.isString())
      return json.getAsString();
    else {
      BigDecimal bigDec = json.getAsBigDecimal();
      // Find out if it is an int type
      try {
        bigDec.toBigIntegerExact();
        try {
          return bigDec.intValueExact();
        } catch (ArithmeticException e) {
        }
        return bigDec.longValue();
      } catch (ArithmeticException e) {
      }
      // Just return it as a double
      return bigDec.doubleValue();
    }
  }

  private Object handleArray(JsonArray json, JsonDeserializationContext context) {
    int size = json.size();
    List list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(deserialize(json.get(i), Object.class, context));
    }
    return list;
  }

  private Object handleObject(JsonObject json, JsonDeserializationContext context) {
    Record map = Record.createForKafka();
    for (Map.Entry<String, JsonElement> entry : json.entrySet())
      map.put(entry.getKey(), deserialize(entry.getValue(), Object.class, context));
    return map;
  }

}