
package com.demo.output.transform;

import com.demo.messages.EventResponse;
import com.demo.util.Util;

public class UpperCaseTransormEventResponse extends EventResponse<UpperCaseTransformContext> {

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
