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

package io.cloudio.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicAdmin.NewTopicBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.cloudio.consumer.BaseConsumer;

public class Util {
  static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  // private static Executor executor = null;
  private static ScheduledThreadPoolExecutor executor = null;
  static Logger logger = LogManager.getLogger(Util.class);
  static volatile boolean started = false;
  public static final Charset UTF8 = StandardCharsets.UTF_8;
  //private static CuratorFramework curatorClient;

  public static void createCompactedTopic(final AdminClient adminClient, String topic, int partitions, boolean inform)
      throws Exception {
    createCompactedTopic(adminClient, topic, partitions, inform, null);
  }

  public static void createCompactedTopic(final AdminClient adminClient, String topic, int partitions, boolean inform,
      Map<String, Object> configs) throws Exception {
    createTopic(adminClient, topic, partitions, true, inform, configs);
  }

  public static void createTopic(final AdminClient adminClient, String code, int partitions) throws Exception {
    createTopic(adminClient, code, partitions, true);
  }

  public static void createTopic(final AdminClient adminClient, final String topic, final int partitions,
      final boolean inform) throws Exception {
    createTopic(adminClient, topic, partitions, false, false, null);
  }

  private static void createTopic(final AdminClient adminClient, String topic, int partitions, boolean compacted,
      boolean inform, Map<String, Object> configs) throws Exception {
    try {
      List<NewTopic> newTopics = new ArrayList<NewTopic>();

      NewTopicBuilder topicBuilder = TopicAdmin.defineTopic(topic).partitions(partitions)
          .replicationFactor((short) 1);
      if (configs != null && configs.size() > 0) {
        topicBuilder.config(configs);
      }
      if (compacted) {
        topicBuilder.compacted();
      }

      NewTopic newTopic = topicBuilder.build();
      logger.info("Creating topic {}", newTopic.toString());
      newTopics.add(newTopic);
      CreateTopicsResult result = adminClient.createTopics(newTopics);
      result.all().get();
    } catch (Exception e) {
      if (e instanceof TopicExistsException || e.getCause() instanceof TopicExistsException) {
        // ignore me
        logger.info("Topic {} already exists!", topic);
        return;
      } else {
        logger.catching(e);
      }
      throw e;
    }
  }

  public static Future<?> submit(Runnable command) {
    return executor.submit(command);
  }

  public static AdminClient getAdminClient(String bootStrapServer) throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
    return AdminClient.create(properties);
  }

  public static List<Map<String, Integer>> getOffsets(String topicName, String groupId, String bootstrapServer,
      boolean isStart) {
    Properties consumerProps = BaseConsumer.getProperties(groupId);
    List<Map<String, Integer>> offsetList = new ArrayList<Map<String, Integer>>();
    consumerProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    try (KafkaConsumer<String, String> list = new KafkaConsumer<>(consumerProps)) {
      // First, check if the topic exists in the list of all topics
      // First, check if the topic exists in the list of all topics
      Map<String, List<PartitionInfo>> topics = list.listTopics();
      List<PartitionInfo> partitionInfos = topics.get(topicName);
      if (partitionInfos == null) {
        logger.warn("Partition information was not found for topic {}", topicName);
        return null;
      } else {
        Collection<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
          TopicPartition partition = new TopicPartition(topicName, partitionInfo.partition());
          partitions.add(partition);
        }
        Map<TopicPartition, Long> offSets = null;
        if (isStart) {
          offSets = list.beginningOffsets(partitions);
        } else {
          offSets = list.endOffsets(partitions);
        }
        Set<Entry<TopicPartition, Long>> offsets = offSets.entrySet();
        for (Entry<TopicPartition, Long> of : offsets) {
          Long endOffset = of.getValue();
          Integer partition = of.getKey().partition();
          HashMap<String, Integer> offset = new HashMap<String, Integer>();
          offset.put("partition", partition);
          offset.put("offset", endOffset.intValue());
          offsetList.add(offset);
        }
      }

    }

    return offsetList;
  }

  public static final void closeQuietly(AutoCloseable autoClosable) {
    if (autoClosable != null) {
      try {
        autoClosable.close();
      } catch (Throwable e) {
      }
    }

  }

  //SELECT '{"fieldName":'||COLUMN_NAME || ', "type":' ||DATA_TYPE || ',"length":' || DATA_LENGTH || ', "scale":'|| DATA_PRECISION || '},' FROM ALL_TAB_COLS  WHERE table_name ='DEPARTMENTS';  
  public List<HashMap<String, Object>> getSchema(String tableName) throws Exception {
    List<HashMap<String, Object>> schema = new ArrayList<HashMap<String, Object>>();
    InputStream inputStream = null;
    String customFilePath = System.getProperty("custom.filepath");
    logger.info("Custom file path - {}", customFilePath);
    if (StringUtil.isBlank(customFilePath)) {
      inputStream = Resources.getResource("spec.json").openStream();
    } else {
      inputStream = new FileInputStream(customFilePath + "/spec.json");
    }
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

  static Properties props = null;

  public static Properties getDBProperties() {
    if (props == null) {
      Properties _props = loadProps();
      props = _props;
    }
    return props;
  }

  public static String getBootstrapServer() {
    if (props == null) {
      Properties _props = loadProps();
      props = _props;
    }
    return props.getProperty("bootstrap.server");
  }

  private static Properties loadProps() {
    try {
      InputStream inputStream = null;
      String customFilePath = System.getProperty("custom.filepath");
      logger.info("Custom file path - {}", customFilePath);
      if (StringUtil.isBlank(customFilePath)) {
        inputStream = Resources.getResource("io.properties").openStream();
      } else {
        inputStream = new FileInputStream(customFilePath + "/io.properties");
      }
      Properties props = new Properties();
      props.load(inputStream);
      return props;
    } catch (Exception e) {
      logger.catching(e);
      throw new CloudIOException(e);
    }

  }

  // date utils
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

  public static String getInsatnceIdentifier() {
    return UUID.randomUUID().toString();
  }

}
