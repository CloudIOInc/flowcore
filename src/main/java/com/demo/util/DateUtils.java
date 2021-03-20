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

package com.demo.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class DateUtils {
  private static final ConcurrentMap<String, DateTimeFormatter> dateTimeFormatterCache = new ConcurrentHashMap<String, DateTimeFormatter>();

  /**
   * <p>
   * Checks if two dates are on the same day ignoring time.
   * </p>
   *
   * @param date1
   *          the first date, not altered, not null
   * @param date2
   *          the second date, not altered, not null
   * @return true if they represent the same day
   * @throws IllegalArgumentException
   *           if either date is <code>null</code>
   */
  public static boolean isSameDay(Date date1, Date date2) {
    if (date1 == null || date2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date1);
    Calendar cal2 = Calendar.getInstance();
    cal2.setTime(date2);
    return isSameDay(cal1, cal2);
  }

  public static boolean isSameYear(Date date1, Date date2) {
    if (date1 == null || date2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date1);
    Calendar cal2 = Calendar.getInstance();
    cal2.setTime(date2);
    return isSameYear(cal1, cal2);
  }

  public static boolean isSameYear(Calendar cal1, Calendar cal2) {
    if (cal1 == null || cal2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    return cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) &&
        cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR);
  }

  public static boolean isSameMonth(Date date1, Date date2) {
    if (date1 == null || date2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date1);
    Calendar cal2 = Calendar.getInstance();
    cal2.setTime(date2);
    return isSameMonth(cal1, cal2);
  }

  public static boolean isSameMonth(Calendar cal1, Calendar cal2) {
    if (cal1 == null || cal2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    return cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) &&
        cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) &&
        cal1.get(Calendar.MONTH) == cal2.get(Calendar.MONTH);
  }

  public static boolean isSameWeek(Date date1, Date date2) {
    if (date1 == null || date2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date1);
    Calendar cal2 = Calendar.getInstance();
    cal2.setTime(date2);
    return isSameWeek(cal1, cal2);
  }

  public static boolean isSameWeek(Calendar cal1, Calendar cal2) {
    if (cal1 == null || cal2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    return cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) &&
        cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) &&
        cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR);
  }

  /**
   * <p>
   * Checks if two calendars represent the same day ignoring time.
   * </p>
   *
   * @param cal1
   *          the first calendar, not altered, not null
   * @param cal2
   *          the second calendar, not altered, not null
   * @return true if they represent the same day
   * @throws IllegalArgumentException
   *           if either calendar is <code>null</code>
   */
  public static boolean isSameDay(Calendar cal1, Calendar cal2) {
    if (cal1 == null || cal2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    return cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) &&
        cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) &&
        cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR);
  }

  /**
   * <p>
   * Checks if a date is today.
   * </p>
   *
   * @param date
   *          the date, not altered, not null.
   * @return true if the date is today.
   * @throws IllegalArgumentException
   *           if the date is <code>null</code>
   */
  public static boolean isToday(Date date) {
    return isSameDay(date, Calendar.getInstance().getTime());
  }

  /**
   * <p>
   * Checks if a calendar date is today.
   * </p>
   *
   * @param cal
   *          the calendar, not altered, not null
   * @return true if cal date is today
   * @throws IllegalArgumentException
   *           if the calendar is <code>null</code>
   */
  public static boolean isToday(Calendar cal) {
    return isSameDay(cal, Calendar.getInstance());
  }

  /**
   * <p>
   * Checks if the first date is before the second date ignoring time.
   * </p>
   *
   * @param date1
   *          the first date, not altered, not null
   * @param date2
   *          the second date, not altered, not null
   * @return true if the first date day is before the second date day.
   * @throws IllegalArgumentException
   *           if the date is <code>null</code>
   */
  public static boolean isBeforeDay(Date date1, Date date2) {
    if (date1 == null || date2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date1);
    Calendar cal2 = Calendar.getInstance();
    cal2.setTime(date2);
    return isBeforeDay(cal1, cal2);
  }

  public static boolean isInFuture(Date date) {
    if (date == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date);
    Calendar cal2 = Calendar.getInstance();
    return isAfterDay(cal1, cal2);
  }

  public static boolean isPastMinutes(Date date, double minutes) {
    return isPastSeconds(date, Double.valueOf(minutes * 60).intValue());
  }

  public static boolean isPastHours(Date date, double hours) {
    return isPastMinutes(date, hours * 60);
  }

  public static boolean isPastDays(Date date, double days) {
    return isPastHours(date, days * 24);
  }

  public static boolean isPastSeconds(Date date, int seconds) {
    if (date == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date);

    Calendar c = Calendar.getInstance();
    c.add(Calendar.SECOND, -1 * seconds);

    return cal1.before(c);
  }

  /**
   * <p>
   * Checks if the first calendar date is before the second calendar date
   * ignoring time.
   * </p>
   *
   * @param cal1
   *          the first calendar, not altered, not null.
   * @param cal2
   *          the second calendar, not altered, not null.
   * @return true if cal1 date is before cal2 date ignoring time.
   * @throws IllegalArgumentException
   *           if either of the calendars are <code>null</code>
   */
  public static boolean isBeforeDay(Calendar cal1, Calendar cal2) {
    if (cal1 == null || cal2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    return cal1.get(Calendar.ERA) < cal2.get(Calendar.ERA) || (cal1.get(Calendar.ERA) <= cal2.get(Calendar.ERA)
        && (cal1.get(Calendar.YEAR) < cal2.get(Calendar.YEAR) || (cal1.get(Calendar.YEAR) <= cal2.get(Calendar.YEAR)
            && cal1.get(Calendar.DAY_OF_YEAR) < cal2.get(Calendar.DAY_OF_YEAR))));
  }

  /**
   * <p>
   * Checks if the first date is after the second date ignoring time.
   * </p>
   *
   * @param date1
   *          the first date, not altered, not null
   * @param date2
   *          the second date, not altered, not null
   * @return true if the first date day is after the second date day.
   * @throws IllegalArgumentException
   *           if the date is <code>null</code>
   */
  public static boolean isAfterDay(Date date1, Date date2) {
    if (date1 == null || date2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(date1);
    Calendar cal2 = Calendar.getInstance();
    cal2.setTime(date2);
    return isAfterDay(cal1, cal2);
  }

  /**
   * <p>
   * Checks if the first calendar date is after the second calendar date
   * ignoring time.
   * </p>
   *
   * @param cal1
   *          the first calendar, not altered, not null.
   * @param cal2
   *          the second calendar, not altered, not null.
   * @return true if cal1 date is after cal2 date ignoring time.
   * @throws IllegalArgumentException
   *           if either of the calendars are <code>null</code>
   */
  public static boolean isAfterDay(Calendar cal1, Calendar cal2) {
    if (cal1 == null || cal2 == null) {
      throw new IllegalArgumentException("The dates must not be null");
    }
    return cal1.get(Calendar.ERA) >= cal2.get(Calendar.ERA) && (cal1.get(Calendar.ERA) > cal2.get(Calendar.ERA)
        || (cal1.get(Calendar.YEAR) >= cal2.get(Calendar.YEAR) && (cal1.get(Calendar.YEAR) > cal2.get(Calendar.YEAR)
            || cal1.get(Calendar.DAY_OF_YEAR) > cal2.get(Calendar.DAY_OF_YEAR))));
  }

  /**
   * <p>
   * Checks if a date is after today and within a number of days in the future.
   * </p>
   *
   * @param date
   *          the date to check, not altered, not null.
   * @param days
   *          the number of days.
   * @return true if the date day is after today and within days in the future .
   * @throws IllegalArgumentException
   *           if the date is <code>null</code>
   */
  public static boolean isWithinDaysFuture(Date date, int days) {
    if (date == null) {
      throw new IllegalArgumentException("The date must not be null");
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    return isWithinDaysFuture(cal, days);
  }

  /**
   * <p>
   * Checks if a date is before today and within a number of days in the past.
   * </p>
   *
   * @param date
   *          the date to check, not altered, not null.
   * @param days
   *          the number of days.
   * @return true if the date day is before today and within days in the past .
   * @throws IllegalArgumentException
   *           if the date is <code>null</code>
   */
  public static boolean isWithinDaysPast(Date date, int days) {
    if (date == null) {
      throw new IllegalArgumentException("The date must not be null");
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    return isWithinDaysPast(cal, days);
  }

  /**
   * <p>
   * Checks if a calendar date is before today and within a number of days in
   * the past.
   * </p>
   *
   * @param cal
   *          the calendar, not altered, not null
   * @param days
   *          the number of days.
   * @return true if the calendar date day is before today and within days in
   *         the past .
   * @throws IllegalArgumentException
   *           if the calendar is <code>null</code>
   */
  public static boolean isWithinDaysPast(Calendar cal, int days) {
    if (cal == null) {
      throw new IllegalArgumentException("The date must not be null");
    }
    Calendar today = Calendar.getInstance();
    Calendar past = Calendar.getInstance();
    past.add(Calendar.DAY_OF_YEAR, -1 & days);
    return isBeforeDay(cal, today) && !isBeforeDay(cal, past);
  }

  /**
   * <p>
   * Checks if a date is before today and within a number of months in the past.
   * </p>
   *
   * @param date
   *          the date to check, not altered, not null.
   * @param months
   *          the number of months.
   * @return true if the date day is before today and within months in the past
   *         .
   * @throws IllegalArgumentException
   *           if the date is <code>null</code>
   */
  public static boolean isWithinMonthsPast(Date date, int months) {
    if (date == null) {
      throw new IllegalArgumentException("The date must not be null");
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    return isWithinMonthsPast(cal, months);
  }

  /**
   * <p>
   * Checks if a calendar date is before today and within a number of months in
   * the past.
   * </p>
   *
   * @param cal
   *          the calendar, not altered, not null
   * @param months
   *          the number of months.
   * @return true if the calendar date day is before today and within months in
   *         the past .
   * @throws IllegalArgumentException
   *           if the calendar is <code>null</code>
   */
  public static boolean isWithinMonthsPast(Calendar cal, int months) {
    if (cal == null) {
      throw new IllegalArgumentException("The date must not be null");
    }
    Calendar today = Calendar.getInstance();
    Calendar past = Calendar.getInstance();
    past.add(Calendar.MONTH, -1 & months);
    return isBeforeDay(cal, today) && !isBeforeDay(cal, past);
  }

  /**
   * <p>
   * Checks if a calendar date is after today and within a number of days in the
   * future.
   * </p>
   *
   * @param cal
   *          the calendar, not altered, not null
   * @param days
   *          the number of days.
   * @return true if the calendar date day is after today and within days in the
   *         future .
   * @throws IllegalArgumentException
   *           if the calendar is <code>null</code>
   */
  public static boolean isWithinDaysFuture(Calendar cal, int days) {
    if (cal == null) {
      throw new IllegalArgumentException("The date must not be null");
    }
    Calendar today = Calendar.getInstance();
    Calendar future = Calendar.getInstance();
    future.add(Calendar.DAY_OF_YEAR, days);
    return isAfterDay(cal, today) && !isAfterDay(cal, future);
  }

  public static boolean isWithinDates(Date date, Date start, Date end) {
    if (date == null || start == null || end == null) {
      throw new IllegalArgumentException("The date must not be null");
    }
    return isAfterDay(date, start) && isBeforeDay(date, end);
  }

  /** Returns the given date with the time set to the start of the day. */
  public static Date getStart(Date date) {
    return clearTime(date);
  }

  /** Returns the given date with the time values cleared. */
  public static Date clearTime(Date date) {
    if (date == null) {
      return null;
    }
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTime();
  }

  public static Date clearMilliSeconds(Date date) {
    if (date == null) {
      return null;
    }
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTime();
  }

  public static Date clearSeconds(Date date) {
    if (date == null) {
      return null;
    }
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTime();
  }

  public static Date add(Date date, int field, int amount) {
    if (date == null) {
      return null;
    }
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(field, amount);
    return c.getTime();
  }

  public static Date addDays(Date date, int days) {
    return add(date, Calendar.DAY_OF_MONTH, days);
  }

  public static Date addHours(Date date, int hours) {
    return add(date, Calendar.HOUR_OF_DAY, hours);
  }

  public static Date addMinutes(Date date, int minutes) {
    return add(date, Calendar.MINUTE, minutes);
  }

  public static Date addSeconds(Date date, int seconds) {
    return add(date, Calendar.SECOND, seconds);
  }

  /**
   * Determines whether or not a date has any time values (hour, minute, seconds
   * or millisecondsReturns the given date with the time values cleared.
   */

  /**
   * Determines whether or not a date has any time values.
   *
   * @param date
   *          The date.
   * @return true iff the date is not null and any of the date's hour, minute,
   *         seconds or millisecond values are greater than zero.
   */
  public static boolean hasTime(Date date) {
    if (date == null) {
      return false;
    }
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    return c.get(Calendar.HOUR_OF_DAY) > 0 || c.get(Calendar.MINUTE) > 0 || c.get(Calendar.SECOND) > 0
        || c.get(Calendar.MILLISECOND) > 0;
  }

  /** Returns the given date with time set to the end of the day */
  public static Date getEnd(Date date) {
    if (date == null) {
      return null;
    }
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.set(Calendar.HOUR_OF_DAY, 23);
    c.set(Calendar.MINUTE, 59);
    c.set(Calendar.SECOND, 59);
    c.set(Calendar.MILLISECOND, 999);
    return c.getTime();
  }

  /**
   * Returns the maximum of two dates. A null date is treated as being less than
   * any non-null date.
   */
  public static Date max(Date d1, Date d2) {
    if (d1 == null && d2 == null) {
      return null;
    }
    if (d1 == null) {
      return d2;
    }
    if (d2 != null) {
      return d1.after(d2) ? d1 : d2;
    }
    return d1;
  }

  /**
   * Returns the minimum of two dates. A null date is treated as being greater
   * than any non-null date.
   */
  public static Date min(Date d1, Date d2) {
    if (d1 == null && d2 == null) {
      return null;
    }
    if (d1 == null) {
      return d2;
    }
    if (d2 != null) {
      return d1.before(d2) ? d1 : d2;
    }
    return d1;
  }

  public static final int SECOND = 1;
  public static final int MINUTE = 60 * SECOND;
  public static final int HOUR = 60 * MINUTE;
  public static final int DAY = 24 * HOUR;
  public static final int MONTH = 30 * DAY;
  public static final int YEAR = 365 * DAY;

  public static String timeAgo(Date date) {
    if (date == null) {
      return "not yet";
    }

    long now = new Date().getTime() / 1000;

    long past = date.getTime() / 1000;

    long delta = now - past;

    if (delta < 0) {
      return "not yet";
    }
    if (delta < 1 * MINUTE) {
      return delta == 1 ? "one second ago" : delta + " seconds ago";
    }
    if (delta < 2 * MINUTE) {
      return "a minute ago";
    }
    if (delta < 45 * MINUTE) {
      return delta / MINUTE + " minutes ago";
    }
    if (delta < 90 * MINUTE) {
      return "an hour ago";
    }
    if (delta < 150 * MINUTE) {
      return "couple hours ago";
    }
    if (delta < 24 * HOUR) {
      return delta / HOUR + " hours ago";
    }
    if (delta < 48 * HOUR) {
      return "yesterday";
    }
    if (delta < 30 * DAY) {
      return delta / DAY + " days ago";
    }
    if (delta < 12 * MONTH) {
      int months = (int) delta / MONTH;
      return months <= 1 ? "one month ago" : months + " months ago";
    } else {
      SimpleDateFormat sdf = new SimpleDateFormat("MMM d, yyyy");
      return sdf.format(date);
    }
  }

  /** The maximum date possible. */
  public static final Date MAX_DATE = new Date(Long.MAX_VALUE);

  public static String format(Date d, String format) {
    DateTimeFormatter formatter = dateTimeFormatterCache.get(format);
    if (formatter == null) {
      formatter = DateTimeFormatter.ofPattern(format);
      formatter.withZone(JsonUtils.UTC);
      dateTimeFormatterCache.put(format, formatter);
    }
    return d.toInstant().atZone(JsonUtils.UTC).format(formatter);
  }

  public static Date parse(String d, String format) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    sdf.setTimeZone(TimeZone.getTimeZone(JsonUtils.UTC));
    return sdf.parse(d);
  }

  public static long getDaysBetween(Date from, Date to) {
    long diff = to.getTime() - from.getTime();
    return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
  }

  public static long getHoursBetween(Date from, Date to) {
    long diff = to.getTime() - from.getTime();
    return TimeUnit.HOURS.convert(diff, TimeUnit.MILLISECONDS);
  }

  public static long getMinutesBetween(Date from, Date to) {
    long diff = to.getTime() - from.getTime();
    return TimeUnit.MINUTES.convert(diff, TimeUnit.MILLISECONDS);
  }

  public static void main(String[] args) {
    //    Calendar from = Calendar.getInstance();
    //
    //    from.add(Calendar.MINUTE, -10);
    //    Calendar to = Calendar.getInstance();
    //
    //    System.out.println(from.getTime() + " : " + to.getTime());
    //    long l = getMinutesBetween(from.getTime(), to.getTime());
    //    System.out.println(l);

    Calendar midnight = Calendar.getInstance();
    System.out.println("Now:" + midnight.getTime());
    midnight.set(Calendar.HOUR_OF_DAY, 0);
    midnight.set(Calendar.MINUTE, 0);
    midnight.set(Calendar.SECOND, 0);
    midnight.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    // midnight.add(Calendar.DATE, 7);
    System.out.println("Last Sunday:" + midnight.getTime());
    midnight.add(Calendar.DATE, 7);
    System.out.println("Next Sunday:" + midnight.getTime());
  }
}
