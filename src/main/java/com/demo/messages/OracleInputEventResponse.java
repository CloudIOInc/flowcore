
package com.demo.messages;

import com.demo.util.Util;

public class OracleInputEventResponse extends EventResponse<OracleInputContext> {

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
