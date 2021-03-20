
package io.cloudio.scale.kafka.eventbus;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BusyCounter {
  private static Logger logger = LogManager.getLogger(BusyCounter.class);
  private final static AtomicLong count = new AtomicLong(0);
  private static final Set<String> busyNames = new HashSet<>();

  public static void busy(String name) {
    long val = count.incrementAndGet();
    if (!busyNames.add(name)) {
      logger.warn("Called busy on {}, when it's already busy!!! {}", name, val);
    } else if (logger.isDebugEnabled()) {
      logger.debug("{} is busy: {}", name, val);
    }
    if (val == 1) {
      IOEventBus.post(BusyEvent.Busy);
    }
    IOEventBus.post(new BusyCountEvent());
  }

  public static void free(String name) {
    long val = count.decrementAndGet();
    if (!busyNames.remove(name)) {
      logger.warn("Called free on {}, when it's not busy!!! {}", name, val);
    } else if (logger.isDebugEnabled()) {
      logger.debug("{} is free: {}", name, val);
    }
    if (val == 0) {
      IOEventBus.post(BusyEvent.Free);
    }
    IOEventBus.post(new BusyCountEvent());
  }

  public static long getCount() {
    return count.get();
  }

}
