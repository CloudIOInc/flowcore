
package io.cloudio.task;

import io.cloudio.messages.Settings;

public class Event <S extends Settings>{
  String wfUid;
  String fromTopic;
  String toTopic;
  S settings;
  
  public String getWfUid() {
    return wfUid;
  }
  public void setWfUid(String wfUid) {
    this.wfUid = wfUid;
  }
  public String getFromTopic() {
    return fromTopic;
  }
  public void setFromTopic(String fromTopic) {
    this.fromTopic = fromTopic;
  }
  public String getToTopic() {
    return toTopic;
  }
  public void setToTopic(String toTopic) {
    this.toTopic = toTopic;
  }
  public S getSettings() {
    return settings;
  }
  public void setSettings(S settings) {
    this.settings = settings;
  }
 
  
  
}
