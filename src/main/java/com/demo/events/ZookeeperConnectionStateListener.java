
package com.demo.events;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZookeeperConnectionStateListener implements ConnectionStateListener {
  static Logger logger = LogManager.getLogger(ZookeeperConnectionStateListener.class);
  private ConnectionState oldState;

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    if (oldState != null) {
      logger.warn("Zookeeper connection state changed from {} to {}", oldState, newState);
    }
    switch (newState) {
    case CONNECTED:
      break;
    case LOST:
      break;
    case READ_ONLY:
      break;
    case RECONNECTED:
      break;
    case SUSPENDED:
      break;
    }
    oldState = newState;
  }

}
