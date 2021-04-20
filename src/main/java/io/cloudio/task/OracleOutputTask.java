
package io.cloudio.task;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.messages.OutputSettings;
import io.cloudio.messages.TaskRequest;
import io.cloudio.util.ReaderUtil;

public abstract class OracleOutputTask extends OutputTask<TaskRequest<OutputSettings>, Data, Data> {
  ReaderUtil readerUtil = new ReaderUtil();
  private static Logger logger = LogManager.getLogger(OracleOutputTask.class);

  public OracleOutputTask(String taskCode) {
    super(taskCode);

  }

  public abstract void onData(TaskRequest<OutputSettings> E, List<Data> D) throws Exception;

  public void handleData(List<Data> data) throws Exception {
    int lastIndex = data.size() - 1;
    Data lastMessage = data.get(lastIndex);
    if (lastMessage.isEnd()) {
      unsubscribeData();
      data.remove(lastIndex);
    }
    this.onData(event, data);
  }

  protected String getInsertQuery(String tableName) throws Exception {
    StringBuilder sb = new StringBuilder();
    StringBuilder placeHolder = new StringBuilder();
    sb.append("insert into ")
        .append(tableName)
        .append("(");
    getSchema(tableName).forEach(field -> {
      logger.info(field);
      sb.append(field.get("fieldName")).append(",");
      placeHolder.append("?,");
    });

    sb.deleteCharAt(sb.length() - 1);
    placeHolder.deleteCharAt(placeHolder.length() - 1);
    sb.append(") values (");
    sb.append(placeHolder.toString());
    sb.append(")");

    return sb.toString();
  }
}
