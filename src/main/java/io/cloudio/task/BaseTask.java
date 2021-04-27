
package io.cloudio.task;

import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import io.cloudio.consumer.SinglePartitionEventConsumer;
import io.cloudio.messages.TaskEndResponse;
import io.cloudio.messages.TaskRequest;
import io.cloudio.producer.Producer;
import io.cloudio.task.Data.EventType;
import io.cloudio.util.CloudIOException;
import io.cloudio.util.Util;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;

public abstract class BaseTask<K, V> {
  public static final String WF_EVENTS_TOPIC = "wf_events";
  private static Logger logger = LogManager.getLogger(BaseTask.class);
  protected static Map<String, Object> DUMMY_MAP = new HashMap<String, Object>();
  private ConcurrentHashMap<String, List<HashMap<String, Object>>> schemaCache = new ConcurrentHashMap<>();
  Util readerUtil = new Util();
  Properties inputProps = null;
  static ExecutorService executorService = Executors.newFixedThreadPool(8);
  protected int partitions;

  protected TaskRequest taskRequest;
  protected String taskCode;
  protected String eventTopic;
  protected SinglePartitionEventConsumer<String, String> taskConsumer;
  protected String groupId;
  private boolean isSendStartResponse;

  public BaseTask(String taskCode) {
    this.taskCode = taskCode;
    this.eventTopic = taskCode;
    this.groupId = taskCode + "-grId";
    addShutdownHook();
  }

  public void start() throws Exception {
    subscribeEvent();
  }

  public abstract void handleEvent() throws Throwable;

  public void handleData(List<Data> data) throws Exception {

  }

  void subscribeEvent() {

    taskConsumer = new SinglePartitionEventConsumer<String, String>(this.groupId, new TopicPartition(eventTopic, 0),
        (BaseTask<String, String>) this) {

      @Override
      public void handleEvent(TopicPartition topicPartition, ConsumerRecord<String, String> message)
          throws Throwable {
        String eventString = message.value();
        task.taskRequest = getTaskRequest(eventString);
        task.handleEvent();
      }

    };

    executorService
        .execute(taskConsumer);
  }

  static Gson gson = new Gson();

  protected TaskRequest getTaskRequest(String eventSting) {
    Type settingsType = new TypeToken<TaskRequest>() {
    }.getType();
    return gson.fromJson(eventSting, settingsType);
  }

  /*
  {
    "appUid": "cloudio",
    "endDate": "2021-04-20T09:51:36.109358Z",
    "executionId": 1,
    "nodeUid": "oracle output",
    "orgUid": "cloudio",
    "outcome": {
      "status": "Success"
    },
    "output": {},
    "startDate": "2021-04-20T09:51:27.109358Z",
    "version": 1,
    "wfInstUid": "56b7c93b-996e-44db-923a-1b75aef76142",
    "wfUid": "2c678365-b3f4-4194-b7ac-262e27c48379"
  }
  */
  protected void sendTaskEndResponse(TaskRequest taskRequest, boolean isError) throws Exception {
    TaskEndResponse response = new TaskEndResponse();
    response.setAppUid(taskRequest.getAppUid());
    response.setEndDate(Util.dateToJsonString(new Date()));
    response.setExecutionId(taskRequest.getExecutionId());
    response.setNodeUid(taskRequest.getNodeUid());
    response.setOrgUid(taskRequest.getOrgUid());
    Map<String, String> outCome = new HashMap<String, String>();
    if (isError) {
      outCome.put("status", "Error");
    } else {
      outCome.put("status", "Success");
    }
    response.setOutcome(outCome);
    response.setOutput(new HashMap<String, Object>());
    response.setStartDate(taskRequest.getStartDate());
    response.setWfInstUid(taskRequest.getWfInstUid());
    response.setWfUid(taskRequest.getWfUid());
    response.setVersion(taskRequest.getVersion());

    try (Producer p = Producer.get()) {
      p.beginTransaction();
      p.send(WF_EVENTS_TOPIC, response);
      p.commitTransaction();
    }
    logger.info("Sending task end response  for - {}-{}-{} ", taskRequest.getNodeType(), taskRequest.getTaskType(),
        taskRequest.getWfInstUid());
  }

  protected List<HashMap<String, Object>> getSchema(String tableName) throws Exception {
    List<HashMap<String, Object>> map = schemaCache.get(tableName);
    if (map == null) {
      map = readerUtil.getSchema(tableName);
      schemaCache.put(tableName, map);
    }
    return map;
  }

  protected Properties getDBProperties() throws Exception {
    if (inputProps == null) {
      inputProps = Util.getDBProperties();
    }
    return inputProps;
  }

  protected void startEvent() {
    taskConsumer.wakeup();
    taskConsumer.start();
  }

  protected void unsubscribeEvent() {
    taskConsumer.close();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          logger.info("Closing resources!!");
          executorService.shutdown();
          executorService.awaitTermination(1, TimeUnit.MINUTES);
          Thread.sleep(1 * 20 * 1000);
        } catch (Exception e) {
          //ignore
        }
      }
    });
  }

  protected void sendEndMessage() throws Exception {
    Data endMessage = new Data();
    endMessage.setEnd(EventType.End);
    List<Map<String, Integer>> offsets = Util.getOffsets(taskRequest.getToTopic(), groupId, Util.getBootstrapServer(),
        false);
    try (Producer p = Producer.get()) {
      p.beginTransaction();
      Iterator<Map<String, Integer>> ite = offsets.listIterator();
      while (ite.hasNext()) {
        Integer partition = ite.next().get("partition");
        p.send(taskRequest.getToTopic(), partition, null, endMessage);
      }
      p.commitTransaction();
      logger.info("Sending end data message for - {}", taskRequest.getToTopic());
    }
  }

  public boolean isSendStartResonse() {
    return isSendStartResponse;
  }

  public void setSendStartResonse(boolean isSendResonse) {
    this.isSendStartResponse = isSendResonse;
  }
  //read base url form io.properties
  //wf/put
  //wf/get

  public <V> void put(String key, V value) {
    Map<String, Object> values = new HashMap<String, Object>();
    values.put("key", key);
    values.put("value", value);
    HttpResponse<JsonNode> response = WFPost("/v1/wf/put", values);
    handlePostError(response);
  }

  public <V> V get(String key) {
    Map<String, Object> values = new HashMap<String, Object>();
    values.put("key", key);
    HttpResponse<JsonNode> response = WFPost("/v1/wf/get", values);
    return handlePostValue(response);
  }

  public <V> void instancePut(String key, V value) {
    Map<String, Object> values = new HashMap<String, Object>();
    values.put("key", key);
    values.put("value", value);
    HttpResponse<JsonNode> response = WFPost(" /v1/wf/instancePut", values);
    handlePostError(response);
  }

  public <V> V instanceGet(String key) {
    Map<String, Object> values = new HashMap<String, Object>();
    values.put("key", key);
    HttpResponse<JsonNode> response = WFPost(" /v1/wf/instanceGet", values);
    return handlePostValue(response);
  }

  private HttpResponse<JsonNode> WFPost(String method, Map<String, Object> values) {
    populateWFRequest(values);
    HttpResponse<JsonNode> response = Unirest.post(Util.getWFEngineUrl() + method)
        .header("X-Application", "cloudio")
        .header("Content-Type", "application/json")
        .body(values)
        .asJson();
    return response;
  }

  private void populateWFRequest(Map<String, Object> values) {
    values.put("token", taskRequest.getToken());
    values.put("executionId", taskRequest.getExecutionId());
    values.put("version", taskRequest.getVersion());
    values.put("nodeUid", taskRequest.getNodeType());
    values.put("wfInstUid", taskRequest.getWfInstUid());
    values.put("wfUid", taskRequest.getWfUid());
  }

  private void handlePostError(HttpResponse<JsonNode> response) {
    String status = response.getBody().getObject().getString("status");
    if ("ERROR".equals(status)) {
      throw new CloudIOException(status);
    }
  }

  private <V> V handlePostValue(HttpResponse<JsonNode> response) {
    handlePostError(response);
    return (V) response.getBody().getObject().get("value");
  }

}
