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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class StringUtil {

  /**
   * Converts the string with a camel case or with underscores and replace it
   * with spaces between each word, and underscores replaced by spaces. For
   * example "javaProgramming" and "JAVA_PROGRAMMING" would both become Java
   * Programming".
   *
   * @param str
   *          The String to convert
   * @return The converted String
   */
  public static String toTitleCase(String str) {
    if (str == null || str.length() == 0) {
      return str;
    }

    StringBuilder result = new StringBuilder();
    /*
     * Pretend space before first character
     */
    char prevChar = ' ';

    /*
     * Change underscore to space, insert space before capitals
     */
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == '_') {
        result.append(' ');
      } else if (prevChar == ' ' && c == ' ') {
        continue;
      } else if (prevChar == ' ' || prevChar == '_') {
        result.append(Character.toUpperCase(c));
      } else if (Character.isUpperCase(c) && !Character.isUpperCase(prevChar)) {
        /*
         * Insert space before start of word if camel case
         */
        result.append(' ');
        result.append(c);
      } else {
        result.append(Character.toLowerCase(c));
      }
      prevChar = c;
    }

    return result.toString().trim();
  }

  public static String toSnakeCase(String str) {
    if (str == null || str.length() == 0) {
      return str;
    }

    StringBuilder result = new StringBuilder();
    /*
     * Pretend space before first character
     */
    char prevChar = ' ';

    /*
     * Change underscore to space, insert space before capitals
     */
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (prevChar == ' ' && c == ' ') {
        continue;
      } else if (Character.isDigit(c)) {
        result.append(c);
        continue;
      } else if (prevChar == ' ' || prevChar == '_') {
        result.append(Character.toLowerCase(c));
      } else if (Character.isUpperCase(c) && !Character.isUpperCase(prevChar)) {
        /*
         * Insert space before start of word if camel case
         */
        result.append('_');
        result.append(Character.toLowerCase(c));
      } else {
        result.append(Character.toLowerCase(c));
      }
      prevChar = c;
    }
    return result.toString().trim();
  }

  public static String toCamelCase(String str) {
    if (str == null || str.length() == 0) {
      return str;
    }

    StringBuilder result = new StringBuilder();

    /*
     * Pretend space before first character
     */
    char prevChar = '0';

    /*
     * Change underscore to space, insert space before capitals
     */
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c != ' ' && c != '_') {
        if (prevChar == ' ' || prevChar == '_') {
          result.append(Character.toUpperCase(c));
        } else {
          result.append(Character.toLowerCase(c));
        }
      }
      prevChar = c;
    }

    return result.toString();
  }

  public static String replaceFirstIgnoreCase(String value, String from, String to) {
    int idx = value.toLowerCase().indexOf(from.toLowerCase());
    if (idx == -1) {
      return value;
    }
    return value.substring(0, idx) + to + value.substring(idx + from.length());
  }

  public static String replaceFirst(String value, String from, String to) {
    int idx = value.indexOf(from);
    if (idx == -1) {
      return value;
    }
    return value.substring(0, idx) + to + value.substring(idx + from.length());
  }

  public static List<String> split(String value, final char delimiter) {
    if (value == null) {
      return null;
    }
    List<String> tokens = new ArrayList<>();
    int len = value.length();
    StringBuilder token = new StringBuilder();
    char c;
    char lastChar = 0;
    for (int i = 0; i < len; i++) {
      c = value.charAt(i);
      if (c == delimiter) {
        if (lastChar == '\\') {
          token.setLength(token.length() - 1);
          token.append(c);
          lastChar = c;
          continue;
        } else {
          tokens.add(token.toString().trim());
          token.setLength(0);
        }
      } else {
        token.append(c);
      }
      lastChar = c;
    }
    if (token.length() > 0) {
      tokens.add(token.toString().trim());
    }
    return tokens;
  }

  public static List<String> splitQuoted(String value, final char delimiter) {
    if (value == null) {
      return null;
    }
    List<String> tokens = new ArrayList<>();
    int len = value.length();
    StringBuilder token = new StringBuilder();
    char c;
    boolean inQuotes = false;
    char lastChar = 0;
    for (int i = 0; i < len; i++) {
      c = value.charAt(i);
      if (c == delimiter && !inQuotes) {
        if (lastChar == '\\') {
          token.setLength(token.length() - 1);
          token.append(c);
          lastChar = c;
          continue;
        } else {
          tokens.add(token.toString().trim());
          token.setLength(0);
        }
      } else if (c == '"') {
        if (lastChar == '\\') {
          token.setLength(token.length() - 1);
          token.append(c);
          lastChar = c;
          continue;
        }
        inQuotes = !inQuotes;
      } else {
        token.append(c);
      }
      lastChar = c;
    }
    if (token.length() > 0) {
      tokens.add(token.toString().trim());
    }
    return tokens;
  }

  public static String replace(String s, String sub, String with) {
    int c = 0;
    int i = s.indexOf(sub, c);
    if (i == -1) {
      return s;
    }

    StringBuffer buf = new StringBuffer(s.length() + with.length());

    buf.append(s, c, i);
    buf.append(with);
    c = i + sub.length();

    if (c < s.length()) {
      buf.append(s, c, s.length());
    }

    return buf.toString();
  }

  public static String getUnicodeString(String str) {
    if (str == null) {
      return null;
    }
    char[] chars = str.toCharArray();
    StringBuffer unicode = new StringBuffer();
    for (int i = 0; i < chars.length; i++) {
      char ch = chars[i];
      String ss = "";
      if (ch < 0x10) {
        ss = "\\u000" + Integer.toHexString(ch);
      } else if (ch < 0x100) {
        ss = "\\u00" + Integer.toHexString(ch);
      } else if (ch < 0x1000) {
        ss = "\\u0" + Integer.toHexString(ch);
      } else {
        ss = "\\u" + Integer.toHexString(ch);
      }
      unicode.append(ss);
    }
    return unicode.toString();
  }

  public static String removeUnicode(String unicode) {
    if (unicode == null) {
      return null;
    }
    StringBuilder output = new StringBuilder("");
    for (int i = 0; i < unicode.length(); i += 4) {
      unicode = unicode.replace("\\u", "");
      String str = unicode.substring(i, i + 4);
      output.append((char) Integer.parseInt(str, 16));
    }
    return output.toString();
  }

  // public static boolean isContains(String source, String subItem) {
  // String pattern = "\\b" + subItem + "\\b";
  // RegExp regExp = RegExp.compile(pattern);
  // MatchResult m = regExp.exec(source);
  // return m != null;
  // }

  public static String f(String message, Object... params) {
    return format(message, params);
  }

  public static String format(String message, Object... params) {
    if (params == null || params.length == 0) {
      return message;
    }
    String[] messages = StringUtils.splitByWholeSeparatorPreserveAllTokens(message, "{}");
    if (messages.length > 1) {
      StringBuilder sb = new StringBuilder();
      int i = -1;
      for (String msg : messages) {
        if (i > -1) {
          sb.append("{").append(i).append("}");
        }
        sb.append(msg);
        i++;
      }
      message = sb.toString();
    }
    return MessageFormat.format(message, params);
  }

  public static String toInitCaps(String source) {
    return StringUtils.capitalize(source);
  }

  public static boolean isBlank(String value) {
    return value == null || value.trim().length() == 0;
  }

  public static boolean isNotBlank(String value) {
    return !isBlank(value);
  }

  public static String appendTrailingSlash(String url) {
    if (!StringUtil.isBlank(url) && !url.endsWith("/")) {
      url = url + "/";
    }
    return url;
  }

  public static void main(String[] args) {
    System.out.println(toTitleCase("ProcessNote"));
    System.out.println();
    System.out.println(toCamelCase("AssetFeed"));
    System.out.println(toSnakeCase("FirstName"));
  }
}
