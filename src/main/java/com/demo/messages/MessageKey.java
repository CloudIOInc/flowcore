
package com.demo.messages;

import java.io.Serializable;

public final class MessageKey implements Serializable {
  private static final long serialVersionUID = 2642628936758869879L;

  public static MessageKey of(String uuid, long runId, int page) {
    return new MessageKey(uuid, runId, page);
  }

  public static MessageKey of(String uuid) {
    return new MessageKey(uuid, -1, -1);
  }

  private transient int hash = 0;
  public final int page;
  public final long runId;
  public final String uuid;

  public MessageKey(String uuid, long runId, int page) {
    this.uuid = uuid;
    this.runId = runId;
    this.page = page;
  }

  @Override
  public boolean equals(Object anObject) {
    if (!(anObject instanceof MessageKey)) {
      return false;
    }
    MessageKey k = (MessageKey) anObject;
    if (runId != k.runId) {
      return false;
    }
    if (page != k.page) {
      return false;
    }
    if (!uuid.equals(k.uuid)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (hash != 0)
      return hash;
    hash = toString().hashCode();
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(uuid);
    sb.append(".").append(runId);
    sb.append(".").append(page);
    return sb.toString();
  }

  public static MessageKey fromString(String value) {
    try {
      String[] parts = value.split("\\.");
      if (parts.length == 3) {
        return MessageKey.of(parts[0], Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
      } else if (parts.length == 2) { // old key
        return MessageKey.of(parts[0], -1, Integer.parseInt(parts[0]));
      } else if (parts.length == 1) { // old key
        return MessageKey.of(parts[0], -1, -1);
      }
    } catch (Exception e) {
      // skip to throw error below
    }
    throw new RuntimeException("Invalid message key " + value);
  }

}
