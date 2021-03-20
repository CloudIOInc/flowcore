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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JsonUtils {

  public static final DateTimeFormatter Lformatter19 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
  public static final DateTimeFormatter Lformatter23 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
  public static final DateTimeFormatter Lformatter26 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

  public static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  public static final DateTimeFormatter formatterZ = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  public static final DateTimeFormatter formatter6 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");
  public static final DateTimeFormatter formatterJS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");
  public static final DateTimeFormatter formatterShort = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  public static final SimpleDateFormat uformatter2 = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
  static final Gson gson = new Gson();
  public static final DateTimeFormatter hformatter = DateTimeFormatter.ofPattern("h:mm a");
  public static final String JSON_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  public static final String JSON_DATE_FORMAT_JS = "yyyy-MM-dd'T'HH:mm:ssZ";
  public static final String JSON_DATE_FORMAT_SHORT = "yyyy-MM-dd";
  public static final String JSON_DATE_FORMAT6 = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ";
  public static final ZoneId UTC = ZoneId.of("UTC");
  // public static final DateTimeFormatter uformatter =
  // DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm a");
  public static final DateTimeFormatter uformatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm a");

  public static void mainx(String[] args) throws SQLException, Exception {
  }

  public static String dateToHourString(Date d) {
    if (d == null) return "null";
    OffsetDateTime date = OffsetDateTime.ofInstant(d.toInstant(), ZoneOffset.systemDefault());
    return date.format(hformatter);
  }

  public static String dateToJsonString(Date d) {
    if (d == null) return "null";
    return dateToJsonString6(d.toInstant());
  }

  public static String dateToJsonStringShort2(Date d) {
    if (d == null) return "null";
    return uformatter2.format(d);
  }

  public static String dateToJsonString6(Date d, String timeZone) {
    if (d == null) return "null";
    return dateToJsonString6(d.toInstant(), timeZone);
  }

  public static String dateToJsonString6(Instant d) {
    return dateToJsonString6(d, UTC);
  }

  public static String dateToJsonString6(Instant d, String timeZone) {
    return dateToJsonString6(d, timeZone == null ? UTC : ZoneId.of(timeZone));
  }

  public static String dateToJsonString6(Instant d, ZoneId timeZone) {
    if (d == null) return "null";
    if (timeZone != null) {
      OffsetDateTime date = OffsetDateTime.ofInstant(d, timeZone);
      return date.format(formatter6);
    } else {
      OffsetDateTime date = OffsetDateTime.ofInstant(d, UTC);
      return date.format(formatter6);
    }
  }

  // public static String dateToJsonString6(LocalDateTime d) {
  // if (d == null) return "null";
  // return d.format(formatter6);
  // }

  // public static String dateToString(Date d) {
  // if (d == null) return "null";
  // return dateToJsonString6(d.toInstant(), null);
  // }

  public static BigDecimal getBigDecimal(JsonObject json, String key) {
    JsonElement value = json.get(key);
    if (value == null || value.isJsonNull()) {
      return null;
    }
    if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber()) {
      return value.getAsBigDecimal();
    }
    String str = value.getAsString();
    return new BigDecimal(str);
  }

  public static String getString(JsonObject json, String key) {
    JsonElement value = json.get(key);
    if (value == null || value.isJsonNull()) {
      return null;
    }
    return value.getAsString();
  }

  public static Timestamp getTimestamp(JsonObject json, String key) {
    JsonElement value = json.get(key);
    if (value == null || value.isJsonNull()) {
      return null;
    }
    String val = value.getAsString();
    return toTimestamp6(val);
  }

  public static Gson gson() {
    return gson;
  }

  public static final JsonObject merge(JsonObject... jsons) {
    JsonObject json = new JsonObject();
    mergeInto(json, jsons);
    return json;
  }

  public static final void mergeInto(JsonObject to, JsonObject... jsons) {
    for (JsonObject from : jsons) {
      for (Entry<String, JsonElement> e : from.entrySet()) {
        to.add(e.getKey(), e.getValue());
      }
    }
  }

  public static final Timestamp toTimestamp(String jsonString) {
    if (jsonString == null) {
      return null;
    }
    if (jsonString.charAt(jsonString.length() - 1) == 'Z') {
      jsonString = jsonString.substring(0, jsonString.length() - 1) + "+00:00";
    }
    OffsetDateTime odt = OffsetDateTime.parse(jsonString, formatter);
    return Timestamp.from(odt.toInstant());
  }

  public static final Timestamp toTimestamp6(String jsonString) {
    if (jsonString == null || jsonString.length() == 0) {
      return null;
    }
    if (jsonString.charAt(jsonString.length() - 1) == 'Z') {
      jsonString = jsonString.substring(0, jsonString.length() - 1) + "+00:00";
    }
    if (jsonString.length() == 25) {
      OffsetDateTime odt = OffsetDateTime.parse(jsonString, formatterJS);
      return Timestamp.from(odt.toInstant());
    } else if (jsonString.length() == 29) {
      OffsetDateTime odt = OffsetDateTime.parse(jsonString, formatter);
      return Timestamp.from(odt.toInstant());
    } else if (jsonString.length() == 28) {
      OffsetDateTime odt = OffsetDateTime.parse(jsonString, formatterZ);
      return Timestamp.from(odt.toInstant());
    } else if (jsonString.length() == 10) {
      jsonString += "T00:00:00.000000" + OffsetDateTime.now().getOffset();
    } else if (jsonString.length() == 19 && !jsonString.contains("T")) {
      int spaceIndex = jsonString.indexOf(" ");
      String date = jsonString.substring(0, spaceIndex);
      jsonString = date + "T00:00:00.000000" + OffsetDateTime.now().getOffset();
      jsonString = jsonString.replaceAll("\\s", "");
    }
    try {
      OffsetDateTime odt = OffsetDateTime.parse(jsonString, formatter6);
      return Timestamp.from(odt.toInstant());
    } catch (Exception e) {
      if (jsonString.length() == 19) {
        LocalDateTime ldt = LocalDateTime.parse(jsonString, Lformatter19);
        return Timestamp.valueOf(ldt);
      } else if (jsonString.length() == 23) {
        LocalDateTime ldt = LocalDateTime.parse(jsonString, Lformatter23);
        return Timestamp.valueOf(ldt);
      } else if (jsonString.length() == 26) {
        LocalDateTime ldt = LocalDateTime.parse(jsonString, Lformatter26);
        return Timestamp.valueOf(ldt);
      } else {
        throw new RuntimeException("Not a supported date string " + jsonString);
      }
    }
  }

  public static boolean isDate(String val) {
    try {
      toTimestamp6(val);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static String dateToString(Date d) {
    if (d == null) return "null";
    OffsetDateTime date = OffsetDateTime.ofInstant(d.toInstant(), ZoneOffset.systemDefault());
    return date.format(uformatter);
  }
}
