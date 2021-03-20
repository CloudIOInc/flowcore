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

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.PathUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.demo.events.Worker;

public class DistributedStartupCoordinator {
  static Logger logger = LogManager.getLogger(DistributedStartupCoordinator.class);
  private final CuratorFramework client;
  private final String startupPath;
  private final String BASE_PATH = "/cloudio/startup/";

  private final Watcher watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      client.postSafeNotify(DistributedStartupCoordinator.this);
    }
  };

  public DistributedStartupCoordinator(String startupPath) throws InterruptedException {
    this.client = Worker.ensureCuratorClient();
    this.startupPath = PathUtils.validatePath(BASE_PATH + startupPath);
  }

  public synchronized void setStart() throws Exception {
    try {
      client.create()
          .creatingParentContainersIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(startupPath);
    } catch (KeeperException.NodeExistsException ignore) {
      // ignore
    }
  }

  public synchronized void shutdown() throws Exception {
    try {
      client.delete()
          .forPath(startupPath);
    } catch (KeeperException.NoNodeException | InterruptedException ignore) {
      // ignore
    } catch (Exception ignore) {
      // ignore
      logger.catching(ignore);
    }
  }

  public synchronized void waitToStart() throws Exception {
    waitToStart(-1, null);
  }

  public synchronized boolean waitToStart(long maxWait, TimeUnit unit) throws Exception {
    long startMs = System.currentTimeMillis();
    boolean hasMaxWait = (unit != null);
    long maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

    boolean result;
    for (;;) {
      result = (client.checkExists().usingWatcher(watcher).forPath(startupPath) != null);
      if (result) {
        break;
      }

      if (hasMaxWait) {
        long elapsed = System.currentTimeMillis() - startMs;
        long thisWaitMs = maxWaitMs - elapsed;
        if (thisWaitMs <= 0) {
          break;
        }
        wait(thisWaitMs);
      } else {
        wait();
      }
    }
    return result;
  }
}
