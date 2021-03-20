
package io.cloudio.scale.kafka.zookeeper;

public class ZookeeperCacheEvent<T> {
  public enum Type {
    Insert, Update, Delete
  }

  public final Type eventType;
  public final T value;
  public final T oldValue;

  public ZookeeperCacheEvent(final Type eventType,
      final T value,
      final T oldValue) {
    this.eventType = eventType;
    this.oldValue = oldValue;
    this.value = value;
  }

  public boolean isDelete() {
    return eventType == Type.Delete;
  }
}
