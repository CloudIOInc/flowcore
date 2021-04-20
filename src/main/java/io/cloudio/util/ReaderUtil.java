
package io.cloudio.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ReaderUtil {
  //SELECT '{"fieldName":'||COLUMN_NAME || ', "type":' ||DATA_TYPE || ',"length":' || DATA_LENGTH || ', "scale":'|| DATA_PRECISION || '},' FROM ALL_TAB_COLS  WHERE table_name ='DEPARTMENTS';  
  public List<HashMap<String, Object>> getSchema(String tableName) throws Exception {
    List<HashMap<String, Object>> schema = new ArrayList<HashMap<String, Object>>();
    InputStream inputStream = Resources.getResource("spec.json").openStream();
    InputStreamReader rd = new InputStreamReader(inputStream);
    JsonElement el = JsonParser.parseReader(rd);
    JsonObject obj = el.getAsJsonObject();
    if (obj.get(tableName) != null) {
      JsonArray jsonarray = obj.get(tableName).getAsJsonArray();
      jsonarray.forEach(element -> {
        JsonObject field = element.getAsJsonObject();
        String fieldName = field.get("fieldName").getAsString();
        String type = field.get("type").getAsString();
        Double length = field.get("length").getAsDouble();
        Double scale = 0d;
        if (field.get("scale") != null) {
          scale = field.get("scale").getAsDouble();
        }
        HashMap<String, Object> _field = new HashMap<String, Object>();
        _field.put("fieldName", fieldName);
        _field.put("type", type);
        _field.put("length", length);
        _field.put("scale", scale);
        schema.add(_field);

      });

      return schema;

    }
    throw new Exception("No schema found for - [" + tableName + "]");
  }

  public Properties getDBProperties() throws Exception {
    InputStream inputStream = Resources.getResource("application.properties").openStream();
    Properties prop = null;
    prop = new Properties();
    prop.load(inputStream);
    return prop;
  }
}