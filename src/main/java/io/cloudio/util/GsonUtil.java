
package io.cloudio.util;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import io.cloudio.messages.OracleEvent;
import io.cloudio.messages.OracleSettings;
import io.cloudio.messages.Settings;
import io.cloudio.messages.TaskRequest;
import io.cloudio.task.Event;
import io.cloudio.task.TransformSettings;

public class GsonUtil {

  static Gson gson = new Gson();

  public static Event<Settings> getEventObject(String json) {

    Type settingsType = new TypeToken<Event<Settings>>() {
    }.getType();

    Event<Settings> event = gson.fromJson(json, settingsType);
    return event;
  }

  public static TaskRequest<Settings> getTransformTaskRequest(String json) {

	    Type settingsType = new TypeToken<TaskRequest<TransformSettings>>() {
	    }.getType();

	    TaskRequest<Settings> event = gson.fromJson(json, settingsType);
	    return event;
	  }
  
  public static OracleEvent<OracleSettings> getDBSettingsEvent(String json) {
    Type dbSettingsType = new TypeToken<OracleEvent<OracleSettings>>() {
    }.getType();

    OracleEvent<OracleSettings> event = gson.fromJson(json, dbSettingsType);
    return event;
  }
}
