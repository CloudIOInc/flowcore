
package com.demo.input;

import com.demo.util.Util;

public class OracleBatchInfo {

  private long offset;
  private long fetchCount;
  private String objectName;
  private long totalCount;

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getFetchCount() {
    return fetchCount;
  }

  public void setFetchCount(long fetchCount) {
    this.fetchCount = fetchCount;
  }

  public String getObjectName() {
    return objectName;
  }

  public void setObjectName(String objectName) {
    this.objectName = objectName;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public void setTotalCount(long totalCount) {
    this.totalCount = totalCount;
  }

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
