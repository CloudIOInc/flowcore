
package com.demo.output.transform;

import java.util.List;

import com.demo.messages.Settings;

public class UpperCaseTransformSettings extends Settings {
  private String objectName;
  private List<String> transforFields;

  public String getObjectName() {
    return objectName;
  }

  public void setObjectName(String objectName) {
    this.objectName = objectName;
  }

  public List<String> getTransforFields() {
    return transforFields;
  }

  public void setTransforFields(List<String> transforFields) {
    this.transforFields = transforFields;
  }

}
