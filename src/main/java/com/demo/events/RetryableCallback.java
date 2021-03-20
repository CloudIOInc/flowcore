
package com.demo.events;

@FunctionalInterface
public interface RetryableCallback {
  public void call() throws Throwable;
}
