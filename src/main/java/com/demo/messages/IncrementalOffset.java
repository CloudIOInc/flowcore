
package com.demo.messages;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import com.demo.util.DateUtils;
import com.demo.util.JsonUtils;

public class IncrementalOffset {

  private Timestamp currentRunTimestamp;
  private BigDecimal idValue;
  private Timestamp lastUpdateDateValue;
  private Boolean queryTimeoutError = false;
  private Boolean connectionError = false;

  public IncrementalOffset() {

  }

  public String formatDate(Date d, String format) {
    return DateUtils.format(d, format);
  }

  public String formatDate(String dateString, String format) {
    Timestamp timestamp = JsonUtils.toTimestamp6(dateString);
    return formatDate(timestamp, format);
  }

  public String formatLastUpdateDateValue(String format) {
    Timestamp ts = lastUpdateDateValue;
    if (ts == null) {
      ts = new Timestamp(0);
    }
    return formatDate(ts, format);
  }

  public String formatLastUpdateDateValueForGithub(String format) {
    Timestamp ts = lastUpdateDateValue;
    if (ts == null) {
      ts = new Timestamp(0);
    }

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(ts.getTime());
    cal.add(Calendar.SECOND, 1);
    Timestamp later = new Timestamp(cal.getTime().getTime());
    return formatDate(later, format);
  }

  public String formatLastUpdateDateValueForJira(String format) {
    Timestamp ts = lastUpdateDateValue;
    if (ts == null) {
      ts = new Timestamp(0);
    }

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(ts.getTime());
    cal.add(Calendar.HOUR, 6);
    Timestamp later = new Timestamp(cal.getTime().getTime());
    return formatDate(later, format);
  }

  public Timestamp getCurrentRunTimestamp() {
    return currentRunTimestamp;
  }

  public BigDecimal getIdValue() {
    return idValue;
  }

  public Timestamp getLastUpdateDateValue() {
    return lastUpdateDateValue;
  }

  public void markAsInitialRun() {
    this.lastUpdateDateValue = null;
    this.idValue = null;
  }

  public void setCurrentRunTimestamp(Timestamp currentRunTimestamp) {
    this.currentRunTimestamp = currentRunTimestamp;
  }

  public boolean setIdValue(BigDecimal id) {
    if (this.idValue != null && id != null && this.idValue.compareTo(id) >= 0) {
      // do not update if this.idValue is already greater than id
      return false;
    }
    this.idValue = id;
    return true;
  }

  public boolean setLastUpdateDateValue(Timestamp lud) {
    if (this.lastUpdateDateValue != null && lud != null && this.lastUpdateDateValue.after(lud)) {
      // do not update if this.lastUpdateDateValue is already greater than lud
      return false;
    }
    this.lastUpdateDateValue = lud;
    return true;
  }

  public Boolean isConnectionError() {
    return connectionError;
  }

  public void setConnectionError(Boolean connectionError) {
    this.connectionError = connectionError;
  }

  public Boolean isQueryTimeoutError() {
    return queryTimeoutError;
  }

  public void setQueryTimeoutError(Boolean queryTimeoutError) {
    this.queryTimeoutError = queryTimeoutError;
  }

}
