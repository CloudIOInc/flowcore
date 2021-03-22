
package com.demo.output.transform;

import com.demo.messages.EventRequest;
import com.demo.util.Util;

public class UpperCaseTransformEventRequest extends EventRequest<UpperCaseTransformSettings, UpperCaseTransformContext> {

  @Override
  public String toString() {
    return Util.getSerializerSkipNulls().toJson(this);
  }

}
