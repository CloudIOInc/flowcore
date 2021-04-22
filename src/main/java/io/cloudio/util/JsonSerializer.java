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

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonSerializer<T> implements Serializer<T> {

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String topic, T t) {
    return getSerializer().toJson(t).getBytes(Charset.defaultCharset());
  }

  @Override
  public void close() {

  }

  private static Gson gsonSerializer;

  static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

  public static final Gson getSerializer() {
    if (gsonSerializer == null) {
      GsonBuilder gsonb = setupGsonBuilder();
      gsonSerializer = gsonb.create();
    }
    return gsonSerializer;
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
}
