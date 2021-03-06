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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.cloudio.consumer.BaseConsumer;

public class KafkaUtil {
  static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  // private static Executor executor = null;
  private static ScheduledThreadPoolExecutor executor = null;
  private static Gson gsonMessageDeserializer, gsonDeserializer, gsonSerializerSkipNulls, gsonSerializer,
      gsonMessageSerializer, gsonPrettySerializer;
  static Logger logger = LogManager.getLogger(KafkaUtil.class);
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

  /*
   * public static void deleteTopic(String topic) throws Exception { try
   * (AdminClient adminClient = getAdminClient()) { DeleteTopicsResult result =
   * adminClient.deleteTopics(Arrays.asList(topic)); result.all().get(); } }
   * 
   * public static void deleteTopicSilent(String topic) { try {
   * deleteTopic(topic); } catch (Exception e) { logger.debug(e); // ignore me } }
   */

  public static Future<?> submit(Runnable command) {
    return executor.submit(command);
  }

  public static <T> Future<T> submit(Callable<T> command) {
    return executor.submit(command);
  }

  public static void execute(Runnable command) {
    executor.execute(command);
  }

  public static void start() throws Exception {
    if (started)
      return;
    started = true;
    ensureExecutor();
    // start status monitor before starting consumers and streams
  }

  private static void ensureExecutor() throws Exception {
    if (executor == null) {
      executor = new ScheduledThreadPoolExecutor(100, Executors.defaultThreadFactory());
    }
  }

  public static ScheduledFuture<?> schedule(Runnable task) {
    return executor.schedule(task, 0, TimeUnit.MILLISECONDS);
  }

  public static ScheduledFuture<?> schedule(Runnable task, long ms) {
    return executor.schedule(task, ms, TimeUnit.MILLISECONDS);
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

  public static void mainA(String[] args) throws Exception {
    // try (AdminClient adminClient = getAdminClient()) {
    // DescribeConsumerGroupsResult result = adminClient
    // .describeConsumerGroups(Collections.singleton("io-actions-group"));
    // ConsumerGroupDescription group =
    // result.describedGroups().get("io-actions-group").get();
    // Collection<MemberDescription> members = group.members();
    // MemberDescription member = members.iterator().next();
    // MemberAssignment assignment = member.assignment();
    // Set<TopicPartition> tp = assignment.topicPartitions();
    // tp.stream().forEach(t -> System.out.println(t.partition()));
    // ListConsumerGroupOffsetsResult aaa =
    // adminClient.listConsumerGroupOffsets("io-actions-group");

    // Map<TopicPartition, OffsetAndMetadata> bbb =
    // aaa.partitionsToOffsetAndMetadata().get();
    // OffsetAndMetadata ccc = bbb.get(new TopicPartition(Topics.inputActions,
    // 0));
    // bbb.entrySet().forEach(e -> logger.info(e.getKey().toString() + ": " +
    // e.getValue().offset()));

    // }
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

  public static ScheduledThreadPoolExecutor getScheduledExecutorService() {
    return executor;
  }

  public static AdminClient getAdminClient(String bootStrapServer) throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
    return AdminClient.create(properties);
  }

  public static List<Map<String, Integer>> getOffsets(String topicName, String groupId, boolean isStart) {
    Properties consumerProps = BaseConsumer.getProperties(groupId);
    List<Map<String, Integer>> offsetList = new ArrayList<Map<String, Integer>>();
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

}
