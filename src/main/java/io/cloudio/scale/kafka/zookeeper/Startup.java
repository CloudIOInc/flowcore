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

package io.cloudio.scale.kafka.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Startup {
  static Logger logger = LogManager.getLogger(Startup.class);

  public static void setInputStarted() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Input started...");
    }
    new DistributedStartupCoordinator("input")
        .setStart();
  }

  public static void waitForInputToStart() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Waiting for Input to be started...");
    }
    new DistributedStartupCoordinator("input")
        .waitToStart();
    if (logger.isInfoEnabled()) {
      logger.info("Input has started...");
    }
  }

  public static void setSchedulerStarted() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Scheduler started...");
    }
    new DistributedStartupCoordinator("scheduler")
        .setStart();
  }

  public static void setMasterStopped() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Master stopped...");
    }
    new DistributedStartupCoordinator("scheduler")
        .shutdown();
    new DistributedStartupCoordinator("topics")
        .shutdown();
  }

  public static void waitForSchedulerToStart() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Waiting for Scheduler to start...");
    }
    new DistributedStartupCoordinator("scheduler")
        .waitToStart();
    if (logger.isInfoEnabled()) {
      logger.info("Scheduler has started...");
    }
  }

  public static void setTopicsInitialized() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Topics initialized...");
    }
    new DistributedStartupCoordinator("topics")
        .setStart();
  }

  public static void waitOnTopicsInitialization() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Waiting for topics to be initialized...");
    }
    new DistributedStartupCoordinator("topics")
        .waitToStart();
    if (logger.isInfoEnabled()) {
      logger.info("Topics have been initialized...");
    }
  }

}
