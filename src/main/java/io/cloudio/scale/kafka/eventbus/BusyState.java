
package io.cloudio.scale.kafka.eventbus;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BusyState {
  private static Logger logger = LogManager.getLogger(BusyState.class);
  private final AtomicInteger busyCounter = new AtomicInteger(0);
  private final String name;
  private final AtomicBoolean busy = new AtomicBoolean();

  public BusyState(String name) {
    this.name = name;
  }

  public void checkAndFree() {
    int count = busyCounter.get();
    if (count == 0) {
      if (busy.compareAndSet(true, false)) {
        BusyCounter.free(name);
      }
    } else if (count < 0) {
      logger.warn("{} busy called multiple times! {}", name, count);
    }
    logger.info("checkAndFree {} busy count {}", name, count);
  }

  public int enter() {
    int count = busyCounter.incrementAndGet();
    if (count == 1) {
      if (busy.compareAndSet(false, true)) {
        BusyCounter.busy(name);
      }
    }
    logger.info("Enter {} busy count {}", name, count);
    return count;
  }

  public int leave() {
    int count = busyCounter.decrementAndGet();
    logger.info("Leave {} busy count {}", name, count);
    return count;
  }

  public void leaveAndFree() {
    this.leave();
    this.checkAndFree();
  }

}
