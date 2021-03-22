
package com.demo.output;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.demo.messages.EventRequest;
import com.demo.messages.Record;
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

  public void initConsumer(KafkaConsumer<String, Record> kafkaConsumer, EventRequest<?, ?> message) {
    Map<String, Integer> offsets = message.getStartOffset();
    for (PartitionInfo partition : kafkaConsumer.partitionsFor(message.getFromTopic())) {
      TopicPartition tp = new TopicPartition(message.getFromTopic(), partition.partition());
      String _tp = tp.toString();
      Integer offset = offsets.get(_tp);
      List<TopicPartition> _tps = new ArrayList<TopicPartition>();
      _tps.add(tp);
      kafkaConsumer.assign(_tps);
      kafkaConsumer.seek(tp, offset);
    }
    // kafkaConsumer.assign(partitions);
  }

}
