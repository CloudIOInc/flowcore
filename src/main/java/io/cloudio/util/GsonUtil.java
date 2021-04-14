
package io.cloudio.util;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import io.cloudio.messages.OracleSettings;
import io.cloudio.messages.Settings;
import io.cloudio.task.Event;

public class GsonUtil {

  static Gson gson = new Gson();

  public static Event<Settings> getEventObject(String json) {

    Type settingsType = new TypeToken<Event<Settings>>() {
    }.getType();

    Event<Settings> event = gson.fromJson(json, settingsType);
    return event;
  }

  public static Event<? extends Settings> getDBSettingsEvent(String json) {
    Type dbSettingsType = new TypeToken<Event<OracleSettings>>() {
    }.getType();

    Event<OracleSettings> event = gson.fromJson(json, dbSettingsType);
    return event;
  }
}
