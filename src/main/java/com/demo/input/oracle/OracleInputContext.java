
package com.demo.input.oracle;

import com.demo.messages.Context;
import com.demo.messages.IncrementalOffset;

public class OracleInputContext extends Context {
  private String status;
  private IncrementalOffset offset;

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public IncrementalOffset getOffset() {
    return offset;
  }

  public void setOffset(IncrementalOffset offset) {
    this.offset = offset;
  }

}
