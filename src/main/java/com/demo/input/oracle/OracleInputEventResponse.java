
package com.demo.input.oracle;

import com.demo.messages.EventResponse;
import com.demo.util.Util;

public class OracleInputEventResponse extends EventResponse<OracleInputContext> {

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
