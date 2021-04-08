package io.cloudio.util;

import java.sql.Timestamp;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

//TODO: combine Serializer and Deserializer api in common class
public class SerializerUtil {

	private static Gson gsonMessageDeserializer, gsonDeserializer, gsonSerializerSkipNulls, gsonSerializer,
			gsonMessageSerializer, gsonPrettySerializer;

	static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

	public static final Gson getSerializer() {
		if (gsonSerializer == null) {
			GsonBuilder gsonb = setupGsonBuilder();
			gsonSerializer = gsonb.create();
		}
		return gsonSerializer;
	}

	public static final GsonBuilder setupGsonBuilder() {
		GsonBuilder gsonb = setupGsonBuilderSkipNulls();
		gsonb.serializeNulls();
		return gsonb;
	}

	public static final GsonBuilder setupGsonBuilderSkipNulls() {
		GsonBuilder gsonb = new GsonBuilder().setDateFormat(DATE_FORMAT);
		gsonb.registerTypeAdapter(Date.class, new GsonUTCDateAdapter());
		gsonb.registerTypeAdapter(Timestamp.class, new GsonUTCDateAdapter());

		gsonb.serializeSpecialFloatingPointValues();
		return gsonb;
	}
}
