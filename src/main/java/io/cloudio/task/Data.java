
package io.cloudio.task;

import java.util.HashMap;

public class Data extends HashMap<String, Object> {

  public enum EventType {
    End, Data
  }

  EventType _eventType;

  public boolean isEnd() {
    return this._eventType == EventType.End;
  }
}
