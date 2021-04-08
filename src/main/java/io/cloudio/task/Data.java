
package io.cloudio.task;

import java.util.TreeMap;

public class Data extends TreeMap<String, Object> {

  public enum EventType {
    End, Data
  }

  EventType _eventType;

  public boolean isEnd() {
    return this._eventType == EventType.End;
  }
}
