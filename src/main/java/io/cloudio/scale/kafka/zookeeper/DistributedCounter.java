
package io.cloudio.scale.kafka.zookeeper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;

import com.demo.events.Worker;

public class DistributedCounter {
  private static final ConcurrentMap<String, DistributedAtomicLong> counterMap = new ConcurrentHashMap<>();

  private static DistributedAtomicLong init(String path) throws Exception {
    DistributedAtomicLong atomicLong = counterMap.get(path);
    if (atomicLong == null) {
      atomicLong = new DistributedAtomicLong(Worker.ensureCuratorClient(), "/cloudio/atomic/" + path,
          new RetryNTimes(60, 300));
      atomicLong.initialize(-1L);
    }
    return atomicLong;
  }

  public static long increment(String path) throws Exception {
    DistributedAtomicLong atomicLong = init(path);
    AtomicValue<Long> val = atomicLong.increment();
    if (val.succeeded()) {
      return val.postValue();
    } else {
      return increment(path);
    }
  }

}
