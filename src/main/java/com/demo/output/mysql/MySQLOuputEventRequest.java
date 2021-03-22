
package com.demo.output.mysql;

import com.demo.messages.EventRequest;
import com.demo.util.Util;

public class MySQLOuputEventRequest extends EventRequest<MySQLOutputSettings, MySQLOutputContext> {

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
