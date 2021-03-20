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
import java.math.BigDecimal;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class GsonBigDecimalAdapter implements JsonSerializer<BigDecimal>, JsonDeserializer<BigDecimal> {

  @Override
  public JsonElement serialize(BigDecimal val, Type type, JsonSerializationContext c) {
    return new JsonPrimitive(val.toString());
  }

  @Override
  public BigDecimal deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext c) {
    try {
      String sval = jsonElement.getAsString();
      return new BigDecimal(sval);
    } catch (Exception e) {
      throw new JsonParseException(e);
    }
  }
}