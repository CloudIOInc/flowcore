
package io.cloudio.util;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;



public class Util {
  private static Gson gsonMessageDeserializer, gsonDeserializer, gsonSerializerSkipNulls, gsonSerializer,
      gsonMessageSerializer, gsonPrettySerializer;
  static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final Charset UTF8 = StandardCharsets.UTF_8;

  public enum RecordStatus {
    Delete, Insert, New, PreQuery, Query, Sync, Update, Upsert, Validate;
  }

  public static final GsonBuilder setupGsonBuilder() {
    GsonBuilder gsonb = setupGsonBuilderSkipNulls();
    gsonb.serializeNulls();
    return gsonb;
  }

  public static final GsonBuilder setupMessageGsonBuilderSkipNulls() {
    GsonBuilder gsonb = setupGsonBuilderSkipNulls();
    return gsonb;
  }

  public static final GsonBuilder setupGsonBuilderSkipNulls() {
    GsonBuilder gsonb = new GsonBuilder().setDateFormat(DATE_FORMAT);
    gsonb.serializeSpecialFloatingPointValues();
    return gsonb;
  }

  public static final Gson getDeserializer() {
    if (gsonDeserializer == null) {
      GsonBuilder gsonb = setupGsonBuilder();
      gsonDeserializer = gsonb.create();
    }
    return gsonDeserializer;
  }

  public static final Gson getMessageDeserializer() {
    if (gsonMessageDeserializer == null) {
      GsonBuilder gsonb = setupMessageGsonBuilderSkipNulls();
      gsonMessageDeserializer = gsonb.create();
    }
    return gsonMessageDeserializer;
  }

  public static final Gson getSerializerSkipNulls() {
    if (gsonSerializerSkipNulls == null) {
      GsonBuilder gsonb = setupGsonBuilderSkipNulls();
      gsonSerializerSkipNulls = gsonb.create();
    }
    return gsonSerializerSkipNulls;
  }

  public static final Gson getMessageSerializerSkipNulls() {
    if (gsonMessageDeserializer == null) {
      GsonBuilder gsonb = setupMessageGsonBuilderSkipNulls();
      gsonMessageDeserializer = gsonb.create();
    }
    return gsonMessageDeserializer;
  }

  public static final Gson getPrettySerializer() {
    if (gsonPrettySerializer == null) {
      GsonBuilder gsonb = setupGsonBuilderSkipNulls();
      gsonb.setPrettyPrinting();
      gsonPrettySerializer = gsonb.create();
    }
    return gsonPrettySerializer;
  }

  public static final Gson getSerializer() {
    if (gsonSerializer == null) {
      GsonBuilder gsonb = setupGsonBuilder();
      gsonSerializer = gsonb.create();
    }
    return gsonSerializer;
  }

  public static final Gson getMessageSerializer() {
    if (gsonMessageSerializer == null) {
      GsonBuilder gsonb = setupMessageGsonBuilderSkipNulls();
      gsonMessageSerializer = gsonb.create();
    }
    return gsonMessageSerializer;
  }

  public static String UUID() {
    return FriendlyId.createFriendlyId();
  }

  static Gson gson = new Gson();

 

}
