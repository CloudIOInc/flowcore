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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.common.eventbus.Subscribe;

public class IOExecutor extends ScheduledThreadPoolExecutor {
  private Continue cont = new Continue();
  CountDownLatch latch = new CountDownLatch(0);

  public IOExecutor(int corePoolSize) {
    super(corePoolSize, new IOThreadFactory("cloudio-executor-"));
    IOEventBus.register(this);
  }

  @Subscribe
  public void onEvent(NodeStateEvent event) {
    if (event.isPauseEvent()) {
      cont.pause();
    } else if (event.isResumeEvent()) {
      cont.resume();
    }
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    try {
      cont.checkIn();
    } catch (InterruptedException e) {
      beforeExecute(t, r);
    }
    super.beforeExecute(t, r);
    BusyCounter.busy(Thread.currentThread().getName());
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    BusyCounter.free(Thread.currentThread().getName());
  }

}