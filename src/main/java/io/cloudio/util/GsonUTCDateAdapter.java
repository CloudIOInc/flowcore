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

package io.cloudio.util;

import java.lang.reflect.Type;
import java.util.Date;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class GsonUTCDateAdapter implements JsonSerializer<Date>, JsonDeserializer<Date> {

  @Override
  public JsonElement serialize(Date date, Type type, JsonSerializationContext c) {
    return new JsonPrimitive(JsonUtils.dateToJsonString(date));
  }

  @Override
  public Date deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext c) {
    try {
      String sval = jsonElement.getAsString();
      return JsonUtils.toTimestamp6(sval);
    } catch (Exception e) {
      throw new JsonParseException(e);
    }
  }
}