
package com.demo.events;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RetryableFunction {
  static Logger logger = LogManager.getLogger(RetryableFunction.class);
  private int maxRetries = Integer.MAX_VALUE;
  private int maxMinutes = 120;
  private final String name;

  public static void main(String[] args) {
    new RetryableFunction("Test").withMaxMinutes(1).call(RetryableFunction::test);
  }

  public static void test() throws Exception {
    throw new Exception("sample error");
  }

  public RetryableFunction(String name) {
    this.name = name;
  }

  public RetryableFunction withMaxMinutes(int maxMinutes) {
    this.maxMinutes = maxMinutes;
    return this;
  }

  public RetryableFunction withMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public <T> T get(RetryableSupplier<T> function) {
    try {
      return function.get();
    } catch (Throwable e) {
      logger.error("{} failed due to {}, will be retried upto {} minutes{}.", name, e.getMessage(), maxMinutes,
          maxRetries == Integer.MAX_VALUE ? "" : " or upto " + maxRetries + " retries");
      return retry(function);
    }
  }

  public void call(RetryableCallback function) {
    this.get(() -> {
      function.call();
      return null;
    });
  }

  private <T> T retry(RetryableSupplier<T> function) throws RuntimeException {
    int retryCounter = 0;
    String error = null;
    long start = System.currentTimeMillis();
    while (retryCounter < maxRetries
        && (System.currentTimeMillis() - start) < TimeUnit.MINUTES.toMillis(maxMinutes)) {
      try {
        return function.get();
      } catch (Throwable ex) {
        retryCounter++;
        logger.error("{} failed on retry {}{}", name, retryCounter, ex.getMessage());
        error = ex.getMessage();
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(Math.min(retryCounter, 300))); // wait up to 5 minutes
        } catch (InterruptedException e) {
        }
        if (retryCounter >= maxRetries) {
          logger.error("Max retries exceeded.");
          break;
        }
      }
    }
    throw new RuntimeException(name + " failed on all of " + retryCounter + " retries");
  }

}
