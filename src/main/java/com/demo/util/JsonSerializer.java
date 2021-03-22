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

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.demo.messages.Record;

public class JsonSerializer<T> implements Serializer<T> {

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String topic, T t) {
    if (t instanceof Record) {
      return Util.getSerializer().toJson(t).getBytes(Util.UTF8);
    }
    return Util.getSerializerSkipNulls().toJson(t).getBytes(Util.UTF8);
  }

  @Override
  public void close() {

  }
}
