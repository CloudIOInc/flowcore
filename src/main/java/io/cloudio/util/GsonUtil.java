package io.cloudio.util;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import io.cloudio.messages.Settings;
import io.cloudio.task.Event;

public class GsonUtil {

	public static Event<Settings> getEventObject(String json) {

		Gson gson = new Gson();
		Type fooType = new TypeToken<Event<Settings>>() {
		}.getType();

		Event<Settings> event = gson.fromJson(json, fooType);
		return event;
	}
}
