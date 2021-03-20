
package io.cloudio.scale.kafka.eventbus;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Continue {
  private AtomicBoolean paused = new AtomicBoolean(false);
  private ReentrantLock pauseLock = new ReentrantLock();
  private Condition unpaused = pauseLock.newCondition();

  public void checkIn() throws InterruptedException {
    if (paused.get()) {
      pauseLock.lock();
      try {
        while (paused.get()) {
          unpaused.await();
        }
      } finally {
        pauseLock.unlock();
      }
    }
  }

  public boolean isPaused() {
    return paused.get();
  }

  public void pause() {
    if (paused.get()) {
      return;
    }
    pauseLock.lock();
    try {
      paused.compareAndSet(false, true);
    } finally {
      pauseLock.unlock();
    }
  }

  public void resume() {
    pauseLock.lock();
    try {
      if (paused.compareAndSet(true, false)) {
        unpaused.signalAll();
      }
    } finally {
      pauseLock.unlock();
    }
  }
}
