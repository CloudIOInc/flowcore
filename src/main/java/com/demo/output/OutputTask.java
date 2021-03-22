
package com.demo.output;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Service
public class OutputTask {

  @Autowired
  private ResourceLoader resourceLoader;

  public JsonArray getSchema(String tableName) throws Exception {
    Resource resource = resourceLoader.getResource("classpath:spec.json");
    InputStream dbAsStream = resource.getInputStream();
    InputStreamReader rd = new InputStreamReader(dbAsStream);
    JsonElement el = JsonParser.parseReader(rd);
    JsonObject obj = el.getAsJsonObject();
    if (obj.get(tableName) != null) {
      return obj.get(tableName).getAsJsonArray();
    }
    throw new Exception("No schema found for - [" + tableName + "]");
  }

}
