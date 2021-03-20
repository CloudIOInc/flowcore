/*
 * Copyright (c) 2014 - present CloudIO Inc.
 * 1248 Reamwood Ave, Sunnyvale, CA 94089
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * CloudIO Inc. ("Confidential Information").  You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with CloudIO.
 */

package com.demo.events;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.gson.JsonSyntaxException;

public class CloudIOGroovyException extends CloudIOException {
  private static String[] IGNORE_PACKAGES = new String[] {
      "com.zaxxer.",
      "gjdk.groovy.",
      "groovy.",
      "java.lang.Thread",
      "java.util.concurrent.",
      "java.util.stream.",
      "org.codehaus.groovy.",
      "org.eclipse.jetty.",
      "sun."
  };
  private static final long serialVersionUID = -4283530444518626662L;

  public static String getMessage(Throwable t) {
    if ((t instanceof CloudIOException
        || t instanceof ExecutionException
        || t instanceof JsonSyntaxException)
        && t.getCause() != null) {
      return getMessage(t.getCause());
    }
    return t.getMessage();
  }

  public static boolean isApplicationClass(String className) {
    for (String groovyPackage : IGNORE_PACKAGES) {
      if (className.startsWith(groovyPackage)) {
        return false;
      }
    }
    return true;
  }

  public static int sanitize(Throwable t) {
    return sanitize(t, -1);
  }

  public static int sanitize(Throwable t, int importLines) {
    int lineNo = -1;
    StackTraceElement[] trace = t.getStackTrace();
    List<StackTraceElement> newTrace = new ArrayList<StackTraceElement>();
    for (StackTraceElement stackTraceElement : trace) {
      if (isApplicationClass(stackTraceElement.getClassName())) {
        if (importLines > 0 && stackTraceElement.getClassName().startsWith("Script1")) {
          stackTraceElement = new StackTraceElement(stackTraceElement.getClassName(),
              stackTraceElement.getMethodName(),
              stackTraceElement.getFileName(),
              stackTraceElement.getLineNumber() - importLines);
          lineNo = stackTraceElement.getLineNumber();
        }
        newTrace.add(stackTraceElement);
      }
    }
    StackTraceElement[] clean = new StackTraceElement[newTrace.size()];
    newTrace.toArray(clean);
    t.setStackTrace(clean);
    if (t.getCause() != null) {
      int causeLineNo = sanitize(t.getCause(), importLines);
      if (lineNo < 0 && causeLineNo > 0) {
        lineNo = causeLineNo;
      }
    }
    return lineNo;
  }

  public static int totalImportLines(String strDefaultImports) {
    String[] lsDefaultImports = strDefaultImports.split("\\r?\\n");//Different operating system has a different new lines  UNIX or Mac \r Windows \r\n
    return lsDefaultImports.length;
  }

  public static CloudIOException with(Exception e, String strDefaultImports) {
    int importLines = totalImportLines(strDefaultImports);
    int lineNo = sanitize(e, importLines);
    CloudIOException ex;
    if (e instanceof CloudIOException) {
      ex = (CloudIOException) e;
    } else {
      ex = CloudIOException.with(e, e.getMessage());
    }
    if (lineNo > -1) {
      ex.setTitle("Groovy Script Error @ Line " + lineNo);
    }
    if (ex.getTitle() == null) {
      ex.setTitle("Groovy Script Error");
    }
    return ex;
  }

}
