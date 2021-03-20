
package com.demo.events;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Monitor;

/**
 * An example leader selector client. Note that {@link LeaderSelectorListenerAdapter} which
 * has the recommended handling for connection state issues
 */
public class Worker extends LeaderSelectorListenerAdapter implements Closeable {
  private final String name;
  private final LeaderSelector leaderSelector;
  private final AtomicInteger leaderCount = new AtomicInteger();
  private final static CountDownLatch latch = new CountDownLatch(1);
  private static CuratorFramework curatorClient;
  private static String zookeeper = "localhost:2181";
  private static final Monitor monitor = new Monitor();
  static Logger logger = LogManager.getLogger(LeaderSelector.class);

  public Worker(String name) throws Exception {
    CuratorFramework client = ensureCuratorClient();
    String path = "/" + name + "/leader";
    this.name = name;

    // create a leader selector using the given path for management
    // all participants in a given leader selection must use the same path
    // ExampleClient here is also a LeaderSelectorListener but this isn't required
    leaderSelector = new LeaderSelector(client, path, this);

    // for most cases you will want your instance to requeue when it relinquishes leadership
    leaderSelector.autoRequeue();
  }

  public void start() throws IOException {
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns immediately
    leaderSelector.start();
  }

  @Override
  public void close() throws IOException {
    leaderSelector.close();
    latch.countDown();
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    // we are now the leader. This method should not return until we want to relinquish leadership

    final int waitSeconds = (int) (5 * Math.random()) + 1;

    System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
    System.out.println(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
      latch.await();
    } catch (InterruptedException e) {
      System.err.println(name + " was interrupted.");
      Thread.currentThread().interrupt();
    } finally {
      System.out.println(name + " relinquishing leadership.\n");
    }
  }

  public static CuratorFramework ensureCuratorClient() throws InterruptedException {
    if (curatorClient == null) {
      monitor.enter();
      try {
        if (curatorClient == null) {
          curatorClient = CuratorFrameworkFactory.newClient(zookeeper,
              new ExponentialBackoffRetry(1000, 3));
          curatorClient.getConnectionStateListenable().addListener(new ZookeeperConnectionStateListener());
          curatorClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
            @Override
            public void unhandledError(String message, Throwable e) {
              logger.error(message);
              logger.catching(e);
            }
          });
          curatorClient.start();
          curatorClient.blockUntilConnected();
        }
      } finally {
        monitor.leave();
      }
    }
    return curatorClient;
  }
}
