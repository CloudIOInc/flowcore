
package com.demo.events;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Monitor;

public class LeaderSelector {
  private static final int CLIENT_QTY = 10;
  private static CuratorFramework curatorClient;
  private static final String PATH = "/examples/leader";
  private static String zookeeper = "localhost:2181";
  private static final Monitor monitor = new Monitor();
  static Logger logger = LogManager.getLogger(LeaderSelector.class);

  public static void main(String[] args) throws Exception {
    // all of the useful sample code is in ExampleClient.java

    System.out.println("Create " + CLIENT_QTY
        + " clients, have each negotiate for leadership and then wait a random number of seconds before letting another leader election occur.");
    System.out.println(
        "Notice that leader election is fair: all clients will become leader and will do so the same number of times.");

    List<CuratorFramework> clients = Lists.newArrayList();
    List<Worker> examples = Lists.newArrayList();
    CuratorFramework client = ensureCuratorClient();
    clients.add(client);
    try {
      for (int i = 0; i < CLIENT_QTY; ++i) {

        Worker example = new Worker("Client #" + i);
        examples.add(example);

        example.start();
      }

      System.out.println("Press enter/return to quit\n");
      new BufferedReader(new InputStreamReader(System.in)).readLine();
    } finally {
      System.out.println("Shutting down...");

      for (Worker exampleClient : examples) {
        CloseableUtils.closeQuietly(exampleClient);
      }
      for (CuratorFramework clientx : clients) {
        CloseableUtils.closeQuietly(clientx);
      }

    }
  }

  private static CuratorFramework ensureCuratorClient() throws InterruptedException {
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