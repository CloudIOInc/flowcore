
package io.cloudio.task2;

import java.util.TreeMap;

public class Data extends TreeMap<String, Object> {

  public enum EventType {
    End, Data
  }

  EventType _eventType;

  public boolean isEnd() {
    if (this.containsKey("_eventType")) {
      return ((String) this.get("_eventType")).equalsIgnoreCase("End");
    }
    return false;
  }

  public void setEnd(EventType type) {
    put("_eventType", type.name());
  }
}
