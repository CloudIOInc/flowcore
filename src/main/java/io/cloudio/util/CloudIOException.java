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

package io.cloudio.util;

import java.io.Serializable;

public class CloudIOException extends RuntimeException implements Serializable {
  private static final long serialVersionUID = 1L;

  public static CloudIOException with(String message, Object... params) {
    return new CloudIOException(StringUtil.format(message, params));
  }

  public static CloudIOException with(Throwable cause, String message, Object... params) {
    return new CloudIOException(StringUtil.format(message, params), cause);
  }

  public static CloudIOException withTitle(String title, String message, Object... params) {
    return new CloudIOException(title, StringUtil.format(message, params));
  }

  String adminMsg;
  String dataSource;
  private String dsFullName;
  String errorDetails;
  private String errorType;
  boolean isAborted = false;
  boolean isInvalidSession = false;
  boolean isTrustedHtml = false;
  boolean isUserError = false;
  private long querySequence;
  boolean sessionExpired;
  private boolean skipFlow;
  String sqlCall;
  String title;

  public CloudIOException() {
    super();
  }

  public CloudIOException(String message) {
    super(message);
  }

  public CloudIOException(String title, String message) {
    this(message);
    this.title = title;
  }

  public CloudIOException(String title, String message, boolean isTrustedHtml) {
    this(title, message);
    this.isTrustedHtml = isTrustedHtml;
  }

  public CloudIOException(String title, String message, String adminMsg) {
    this(title, message);
    this.adminMsg = adminMsg;
  }

  public CloudIOException(String title, String message, String adminMsg, Throwable cause) {
    this(message, cause);
    this.title = title;
    this.adminMsg = adminMsg;
  }

  public CloudIOException(String title, String message, Throwable cause) {
    this(message, cause);
    this.title = title;
  }

  public CloudIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public CloudIOException(Throwable cause) {
    super(cause);
  }

  public String getAdminMsg() {
    return adminMsg;
  }

  public String getDataSource() {
    return dataSource;
  }

  public String getDsFullName() {
    return dsFullName;
  }

  public String getErrorDetails() {
    return errorDetails;
  }

  public String getErrorType() {
    return errorType;
  }

  public long getQuerySequence() {
    return querySequence;
  }

  public String getSqlCall() {
    return sqlCall;
  }

  public String getTitle() {
    return title;
  }

  public boolean isAborted() {
    return isAborted;
  }

  public boolean isInvalidSession() {
    return isInvalidSession;
  }

  public boolean isSessionExpired() {
    return sessionExpired;
  }

  public boolean isSkipFlow() {
    return skipFlow;
  }

  public boolean isTrustedHtml() {
    return isTrustedHtml;
  }

  public boolean isUserError() {
    return isUserError;
  }

  public String print() {
    return (getTitle() != null ? getTitle() + ": " : "") + getMessage();
  }

  public void setAborted(boolean isAborted) {
    this.isAborted = isAborted;
  }

  public void setAdminMsg(String adminMsg) {
    this.adminMsg = adminMsg;
  }

  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  public void setDsFullName(String dsFullName) {
    this.dsFullName = dsFullName;
  }

  public void setErrorDetails(String errorDetails) {
    this.errorDetails = errorDetails;
  }

  public void setErrorType(String errorType) {
    this.errorType = errorType;
  }

  public void setInvalidSession(boolean isInvalidSession) {
    this.isInvalidSession = isInvalidSession;
  }

  public void setQuerySequence(long querySequence) {
    this.querySequence = querySequence;
  }

  public void setQuerySequenceq(long querySequence) {
    this.querySequence = querySequence;
  }

  public void setSessionExpired(boolean sessionExpired) {
    this.sessionExpired = sessionExpired;
  }

  public void setSkipFlow(boolean skipFlow) {
    this.skipFlow = skipFlow;
  }

  public void setSqlCall(String sqlCall) {
    this.sqlCall = sqlCall;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setTrustedHtml(boolean isTrustedHtml) {
    this.isTrustedHtml = isTrustedHtml;
  }

  public void setUserError(boolean isUserError) {
    this.isUserError = isUserError;
  }

}
