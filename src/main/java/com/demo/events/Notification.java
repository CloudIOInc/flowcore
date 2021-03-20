
package com.demo.events;

import java.sql.Timestamp;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Notification {
  static Logger logger = LogManager.getLogger(Notification.class);

  public enum Type {
    danger, info, primary, success, warning
  }

  public static void error(String flow, String key, String message, String body) {
    new Notification(Type.danger, flow, key, message, body, "Error").send();
  }

  public static void info(String flow, String key, String message, String messageAction) {
    info(flow, key, message, null, messageAction);
  }

  public static void info(String flow, String key, String message, String body, String messageAction) {
    new Notification(Type.info, flow, key, message, body, messageAction).send();
  }

  public static void warn(String flow, String key, String message, String messageAction) {
    warn(flow, key, message, null, messageAction);
  }

  public static void warn(String flow, String key, String message, String body, String messageAction) {
    new Notification(Type.warning, flow, key, message, body, messageAction).send();
  }

  String body;
  String flow;
  String key;
  String subject;
  Timestamp time = Timestamp.from(Instant.now());
  Type type;
  String messageAction;

  public Notification() {
  }

  private Notification(Type type, String flow, String key, String subject, String body, String messageAction) {
    this.key = key;
    this.type = type;
    this.subject = subject;
    this.body = body;
    this.flow = flow;
    this.messageAction = messageAction;
  }

  public String getKey() {
    return key;
  }

  public void send() {
    try {
      StringBuilder sb = new StringBuilder();
      if (flow != null) {
        sb.append("[flow: ").append(flow).append("] ");
      }
      if (key != null) {
        sb.append("[").append(key).append("] ");
      }
      sb.append(subject);
      logger.info(sb.toString());
      //ErrorHandler.sendEmailAlert(sb.toString(), body == null ? sb.toString() : body);

    } catch (

    Exception e) {
      // ignore me... we should not throw any error from here
      logger.catching(e);
    }
  }

}
