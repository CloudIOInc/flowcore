
package io.cloudio.scale.kafka.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;

import com.demo.events.RetryableFunction;
import com.demo.events.Worker;
import com.google.common.util.concurrent.Monitor;

public class ZookeeperCache<T> implements AutoCloseable {
  private final String PATH;
  private CuratorFramework client;
  private final Map<String, ZookeeperCacheEvent<T>> _cache = Collections.synchronizedMap(new HashMap<>());
  private TreeCache cache;
  private final Map<String, List<BiConsumer<String, ZookeeperCacheEvent<T>>>> _listeners = Collections
      .synchronizedMap(new HashMap<>());
  private final Monitor monitor = new Monitor();
  private boolean initialized = false;
  private CountDownLatch latch = new CountDownLatch(1);

  public ZookeeperCache(String path) {
    this.PATH = path;
  }

  private void inform(String key, ZookeeperCacheEvent<T> value) {
    monitor.enter();
    try {
      List<BiConsumer<String, ZookeeperCacheEvent<T>>> list = _listeners.remove(key);
      if (list != null) {
        Iterator<BiConsumer<String, ZookeeperCacheEvent<T>>> itr = list.iterator();
        while (itr.hasNext()) {
          BiConsumer<String, ZookeeperCacheEvent<T>> val = itr.next();
          val.accept(key, value);
          itr.remove();
        }
      }
    } finally {
      monitor.leave();
    }
  }

  public ZookeeperCacheEvent<T> addCallbackOnceListener(String key,
      BiConsumer<String, ZookeeperCacheEvent<T>> callback) {
    monitor.enter();
    try {
      List<BiConsumer<String, ZookeeperCacheEvent<T>>> list = _listeners.get(key);
      if (list == null) {
        list = new ArrayList<>();
        _listeners.put(key, list);
      }
      list.add(callback);
      return _cache.get(key);
    } finally {
      monitor.leave();
    }
  }

  public void removeCallbackOnceListener(String key, BiConsumer<String, ZookeeperCacheEvent<T>> callback) {
    monitor.enter();
    try {
      List<BiConsumer<String, ZookeeperCacheEvent<T>>> list = _listeners.get(key);
      if (list != null) {
        list.remove(callback);
      }
    } finally {
      monitor.leave();
    }
  }

  public void put(String key, T value) throws Exception {
    client.create()
        .orSetData()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(PATH + "/" + key, serialize(value));
  }

  public void remove(String key) {
    new RetryableFunction("Remove ZK Path").call(() -> {
      client.delete().quietly().deletingChildrenIfNeeded().forPath(PATH + "/" + key);
    });
  }

  public ZookeeperCacheEvent<T> get(String path) {
    return _cache.get(path);
  }

  private byte[] serialize(T obj) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream os = new ObjectOutputStream(out);
    os.writeObject(obj);
    return out.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private T deserialize(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    ObjectInputStream is = new ObjectInputStream(in);
    return (T) is.readObject();
  }

  public void start() throws Exception {
    monitor.enter();
    try {
      if (client != null) {
        return;
      }
      client = Worker.ensureCuratorClient();
      cache = TreeCache.newBuilder(client, PATH).build();
      cache.getListenable().addListener((c, event) -> {
        String key = null;
        if (event.getData() != null && event.getData().getPath().length() > PATH.length()) {
          key = event.getData().getPath().substring(PATH.length() + 1);
        }
        monitor.enter();
        try {
          switch (event.getType()) {
          case NODE_ADDED:
            if (key != null) {
              T value = deserialize(event.getData().getData());
              ZookeeperCacheEvent<T> e = new ZookeeperCacheEvent<T>(ZookeeperCacheEvent.Type.Insert, value,
                  null);
              _cache.put(key, e);
              if (initialized) {
                inform(key, e);
              }
            }
            break;
          case NODE_UPDATED:
            if (key != null) {
              T value = deserialize(event.getData().getData());
              ZookeeperCacheEvent<T> oldEvent = _cache.get(key);
              ZookeeperCacheEvent<T> e = new ZookeeperCacheEvent<T>(ZookeeperCacheEvent.Type.Update, value,
                  oldEvent.value);
              _cache.put(key, e);
              if (initialized) {
                inform(key, e);
              }
            }
            break;
          case NODE_REMOVED:
            if (key != null) {
              ZookeeperCacheEvent<T> oldEvent = _cache.get(key);
              ZookeeperCacheEvent<T> e = new ZookeeperCacheEvent<T>(ZookeeperCacheEvent.Type.Delete, null,
                  oldEvent.value);
              _cache.put(key, e);
              if (initialized) {
                inform(key, e);
              }
            }
            break;
          case INITIALIZED:
            initialized = true;
            latch.countDown();
            break;
          default:
          }
          // System.out.println(event.getType() + "-" + (event.getData() == null ? "null" : event.getData().getPath()));
        } finally {
          monitor.leave();
        }
      });
      cache.start();
    } finally {
      monitor.leave();
    }
    latch.await();
  }

  @Override
  public void close() {
    cache.close();
  }

}