
package com.demo.events;

@FunctionalInterface
public interface RetryableSupplier<T> {
  public T get() throws Throwable;
}
