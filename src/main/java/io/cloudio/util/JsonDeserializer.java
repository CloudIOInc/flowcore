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

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonDeserializer<T> implements Deserializer<T> {

  private Class<T> deserializedClass;

  private static Gson gsonDeserializer;
  static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

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
        deserializedClass = (Class<T>) map.get("keySerializedClass");
      } else {
        deserializedClass = (Class<T>) map.get("serializedClass");
      }
    }
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String str = new String(bytes, Charset.defaultCharset());
    return getDeserializer().fromJson(str, deserializedClass);
  }

  public static final Gson getDeserializer() {
    if (gsonDeserializer == null) {
      GsonBuilder gsonb = setupGsonBuilder();
      gsonDeserializer = gsonb.create();
    }
    return gsonDeserializer;
  }

  public static final GsonBuilder setupGsonBuilder() {
    GsonBuilder gsonb = setupGsonBuilderSkipNulls();
    gsonb.serializeNulls();
    return gsonb;
  }

  public static final GsonBuilder setupGsonBuilderSkipNulls() {
    GsonBuilder gsonb = new GsonBuilder().setDateFormat(DATE_FORMAT);
    gsonb.registerTypeAdapter(Date.class, new GsonUTCDateAdapter());
    gsonb.registerTypeAdapter(Timestamp.class, new GsonUTCDateAdapter());

    gsonb.serializeSpecialFloatingPointValues();
    return gsonb;
  }

  @Override
  public void close() {

  }
}
