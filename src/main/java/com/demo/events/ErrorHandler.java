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

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.demo.util.JsonUtils;
import com.google.gson.JsonSyntaxException;

public class ErrorHandler {
  static Logger logger = LogManager.getLogger(ErrorHandler.class);

  public static String getMessage(Throwable t) {
    if ((t instanceof CloudIOException
        || t instanceof ExecutionException
        || t instanceof JsonSyntaxException)
        && t.getCause() != null) {
      return getMessage(t.getCause());
    }
    String msg = t.getMessage();
    if (msg == null) {
      msg = t.getClass().getSimpleName();
    }
    return msg;
  }

  public static String getMessageHTML(String err, Throwable ex) {
    final StringBuilder sb = new StringBuilder(err);
    sb.append("<br/>Exception: ").append(ex.getClass().getSimpleName());
    if (!(ex instanceof NullPointerException || ex instanceof SQLException)) {
      sb.append("<br/>Error: ").append(ex.getMessage());
    }
    if (ex.getCause() != null) {
      sb.append("<br/><br/><b>Cause</b><br/>Exception: ").append(ex.getCause().getClass().getSimpleName());
      sb.append("<br/>Error: ").append(ex.getCause().getMessage());
    }
    sb.append("<br/><br/>Time : ").append(Calendar.getInstance().getTime());
    sb.append("<p style='color:#FF0000;'>").append(ex.getClass().getName()).append("</p><p>")
        .append(ex.getMessage()).append("</p>");

    if (ex instanceof CloudIOException && ((CloudIOException) ex).getAdminMsg() != null) {
      sb.append("<br/>").append(((CloudIOException) ex).getAdminMsg());
    }

    if (ex instanceof CloudIOException && ex.getCause() == null) {
      // functional error... no need for stack trace
    } else {
      CloudIOGroovyException.sanitize(ex);
      String[] s = ExceptionUtils.getStackFrames(ex);
      sb.append("<br/><br/><b>Stack Trace:</b>");
      sb.append(String.join("<br/>", s));
    }

    return sb.toString();
  }

  public static String getMessageStackHTML(Throwable ex) {
    final StringBuilder sb = new StringBuilder();

    if (ex instanceof CloudIOException && ex.getCause() == null) {
      // functional error... no need for stack trace
      if (((CloudIOException) ex).getErrorDetails() != null) {
        sb.append("<p><b>Error Details:</b></p>");
        sb.append(((CloudIOException) ex).getErrorDetails());
      }
    } else {
      CloudIOGroovyException.sanitize(ex);
      String[] s = ExceptionUtils.getStackFrames(ex);
      sb.append("<p><b>Stack Trace:</b></p>");
      sb.append(String.join("<br/>", s));
    }

    return sb.toString();
  }

  public static String genBody(String subject, Throwable e, Integer retryCount,
      Timestamp firstTs,
      Timestamp lastTs, String flowName, long runId) throws Exception {
    String errorMesage = getMessageHTML("", e);
    StringBuilder body = new StringBuilder("<div>");
    HTML table = new HTML(body);
    table.startTable(new String[] { "Run Id", "Flow Name", "Retry Count", "First Try", "Last Try" });
    table.startRow()
        .tdCenter(String.valueOf(runId))
        .tdCenter(flowName)
        .tdCenter(retryCount.intValue())
        .tdCenter(JsonUtils.dateToString(firstTs))
        .tdCenter(JsonUtils.dateToHourString(lastTs))
        .endRow();
    table.endTable().alert("For more details. Check online!");
    HTML messagetable = new HTML(body);
    messagetable.startTable(new String[] { "Message" });
    messagetable.startRow()
        .td(errorMesage)
        .endRow();
    return body.toString();
  }

  public static void sendError(String subject, Exception e, Integer retryCount,
      Timestamp firstTs,
      Timestamp lastTs, String flowName, long runId) throws Exception {
    //sendEmailAlert(subject, genBody(subject, e, retryCount, firstTs, lastTs, flowName, runId));
  }

  //  public static void sendEmailAlert(String subject, String body) throws Exception {
  //    List<String> toList = getAdminEmails();
  //    if (toList != null) {
  //      EmailUtil.sendEmail(EmailUtil.getProperties(), toList, subject, body);
  //    }
  //  }

}
