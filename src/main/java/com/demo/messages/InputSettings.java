
package com.demo.messages;

public class InputSettings {

  private String objectName;
  private String type;
  private String baseUrl;
  private String userName;
  private String password;
  private long partition;

  public String getObjectName() {
    return objectName;
  }

  public void setObjectName(String eventName) {
    this.objectName = eventName;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public long getPartition() {
    return partition;
  }

  public void setPartition(long partition) {
    this.partition = partition;
  }

}
