
package com.demo.events;

import java.io.Serializable;

public class JobConsumerStatus implements Serializable {
  private static final long serialVersionUID = 1664120214327740681L;

  public enum Status {
    Running, Complete
  }

  final Status status;
  final String uid;
  final long startOffset;
  final long endOffset;
  final String consumerName;

  public JobConsumerStatus(String consumerName, String uid, Status status, long startOffset, long endOffset) {
    this.status = status;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.uid = uid;
    this.consumerName = consumerName;
  }

  @Override
  public String toString() {
    return consumerName + "-" + uid + "-" + status.toString() + "-" + startOffset + "-" + endOffset;
  }

}
