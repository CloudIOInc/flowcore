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

package io.cloudio.scale.kafka.eventbus;

import com.google.common.eventbus.EventBus;

public class IOEventBus {
  private static final EventBus eventBus = new EventBus();

  public static void register(Object listener) {
    eventBus.register(listener);
  }

  public static void unregister(Object listener) {
    eventBus.unregister(listener);
  }

  public static void post(Object event) {
    eventBus.post(event);
  }

}
