
package com.demo.output.mysql;

import com.demo.messages.EventResponse;
import com.demo.util.Util;

public class MySQLOutputEventResponse extends EventResponse<MySQLOutputContext> {

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
