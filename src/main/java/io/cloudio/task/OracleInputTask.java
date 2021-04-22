
package io.cloudio.task;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.messages.TaskRequest;
import io.cloudio.producer.Producer;
import io.cloudio.util.Util;

public abstract class OracleInputTask extends InputTask {
  private static Logger logger = LogManager.getLogger(OracleInputTask.class);
  private static final String ORACLE_SUBTASK_STATUS = "oracle_subtask_status";
  private static final String ORACLE_SUB_TASKS = "oracle_sub_tasks";
  private Producer producer;

  public void createTopics() throws Exception {
    createTopic(ORACLE_SUB_TASKS, bootStrapServer, partitions);
    createTopic(ORACLE_SUBTASK_STATUS, bootStrapServer, partitions);

  }

  protected OracleInputTask(String taskCode) throws Exception {
    super(taskCode);
  }

  public abstract Map<String, Object> executeTask(Map<String, Object> inputParams, Map<String, Object> outputParams,
      Map<String, Object> inputState) throws Exception;

  public void start(String bootStrapServer) throws Exception {
    super.start(bootStrapServer);
  }

  @Override
  public void handleData(TaskRequest taskRequest) throws Exception {
    boolean isError = false;
    try {
      sendTaskStartResponse(taskRequest, groupId);
      producer = Producer.get();
      producer.beginTransaction();
      executeTask(taskRequest.getInputParams(), taskRequest.getOutputParams(),
          taskRequest.getInputState());
      producer.commitTransaction();
    } catch (Exception e) {
      producer.abortTransactionQuietly();
      logger.catching(e);
      isError = false;
      throw e;
    } finally {
      Util.closeQuietly(producer);
      sendTaskEndResponse(taskRequest, isError);
    }
  }

  public void post(Data data) throws Exception {
    producer.send(taskRequest.getToTopic(), data);
  }

  protected Data populateData(ResultSet rs, String tableName) throws Exception {
    Data d = new Data();
    List<HashMap<String, Object>> schema = getSchema(tableName);
    for (int i = 0; i < schema.size(); i++) {
      Map<String, Object> field = schema.get(i);
      String fieldName = (String) field.get("fieldName");
      Object obj = rs.getObject(fieldName);
      d.put(fieldName, obj);
    }
    return d;
  }

  //read base url form io.properties
  //wf/put
  //wf/get

  public <V> void put(String key, V value) {
    //add unirest

    // pass the token from taskrequest
    // v.tostring() - 
    // invoke rest api to store
  }

  public <V> V get(String key) {
    return null;
    // pass the token from taskrequest
    // invoke rest api to get the data
    // string -> V
  }

  public <V> void instancePut(String key, V value) {
    // v.tostring() - 
    // invoke rest api to store
  }

  public <V> V instanceGet(String key) {
    return null;
    // invoke rest api to get the data
    // string -> V
  }

}
