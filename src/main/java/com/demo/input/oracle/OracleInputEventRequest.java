
package com.demo.input.oracle;

import com.demo.messages.EventRequest;
import com.demo.util.Util;

public class OracleInputEventRequest extends EventRequest<OracleInputSettings, OracleInputContext> {

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
