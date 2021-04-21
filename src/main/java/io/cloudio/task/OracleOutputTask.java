
package io.cloudio.task;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.messages.OutputSettings;
import io.cloudio.messages.TaskRequest;
import io.cloudio.util.ReaderUtil;

public abstract class OracleOutputTask extends OutputTask<TaskRequest<OutputSettings>, Data, Data> {
  ReaderUtil readerUtil = new ReaderUtil();
  private static Logger logger = LogManager.getLogger(OracleOutputTask.class);
  public static final char QUOTE_CHAR = '"';

  public OracleOutputTask(String taskCode) {
    super(taskCode);

  }

  public abstract void onData(TaskRequest<OutputSettings> E, List<Data> D) throws Exception;

  public void handleData(List<Data> data) throws Exception {
    int lastIndex = data.size() - 1;
    Data lastMessage = data.get(lastIndex);
    if (lastMessage.isEnd()) {
      logger.info("Got end event", data.size());
      unsubscribeData();
      data.remove(lastIndex);
    }
    this.onData(event, data);
    logger.info("Processed {} events!", data.size());
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

  public String getCreateTableScript(final Connection con, List<Map<String, Object>> columns,
      String tableName)
      throws SQLException {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(QUOTE_CHAR).append(tableName).append(QUOTE_CHAR).append(" (");
    for (int i = 0; i < columns.size(); i++) {
      Map<String, Object> column = columns.get(i);
      if (i != 0) {
        sb.append(", ");
      }
      String columnName = (String) column.get("fieldName");
      appendColumn(con, sb, column, columnName);
    }
    sb.append(")");
    return sb.toString();
  }

  public void appendColumn(final Connection con, final StringBuilder sb, Map<String, Object> column,
      String columnName) {
    sb.append(QUOTE_CHAR);
    sb.append(columnName);
    sb.append(QUOTE_CHAR);
    int maxLength = Integer.parseInt((String) column.get("length"));
    int sacle = Integer.parseInt((String) column.get("scale"));
    String type = (String) column.get("scale");
    boolean isPrimaryKey = "Y".equals(column.get("isPrimaryKey")) ? false : true;
    boolean isRequired = "Y".equals(column.get("isRequired")) ? false : true;
    String dataType = convertDataType(type, maxLength, isPrimaryKey);
    sb.append(" ").append(dataType);
    switch (dataType) {
    case "VARCHAR":
    case "VARCHAR2":
    case "CHAR":
    case "RAW":
    case "NCHAR":
    case "NVARCHAR2":
      sb.append("(").append(maxLength).append(")");
      break;
    case "DECIMAL":
    case "NUMERIC":
    case "DOUBLE":
    case "NUMBER":
      if (maxLength > 0) {
        sb.append("(").append(maxLength);
        if (sacle > 0) {
          sb.append(",").append(sacle);
        }
        sb.append(")");
      }
      if (isRequired) {
        sb.append(" NOT NULL");
      }
      break;
    default:
      if (isRequired) {
        sb.append(" NOT NULL");
      }
      break;
    }
  }

  public String convertDataType(String dataType, Integer dataLength, boolean primaryKey) {
    if (isVarchar(dataType)) {
      return dataLength.intValue() > 4000 && !primaryKey ? "CLOB" : "VARCHAR2";
    } else if (isNumber(dataType)) {
      return "NUMBER";
    } else if (isDate(dataType)) {
      return "DATE";
    } else if (isDateTime(dataType)) {
      return "TIMESTAMP(6)";
    } else if (isClob(dataType)) {
      return "CLOB";
    } else if (isBoolean(dataType)) {
      return "VARCHAR2(5)";
    } else if (isChar(dataType)) {
      return "CHAR";
    } else {
      return dataType;
    }
  }

  public boolean isVarchar(String dataType) {
    switch (dataType.toUpperCase()) {
    case "VARCHAR":
    case "VARCHAR2":
    case "TINYTEXT":
      return true;
    default:
      return false;
    }
  }

  public boolean isChar(String dataType) {
    switch (dataType.toUpperCase()) {
    case "CHAR":
    case "BPCHAR":
      return true;
    default:
      return false;
    }
  }

  public boolean isClob(String dataType) {
    switch (dataType.toUpperCase()) {
    case "CLOB":
    case "TEXT":
    case "MEDIUMTEXT":
    case "LONGTEXT":
    case "JSON":
      return true;
    default:
      if (dataType.toUpperCase().contains("JSON")) {
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean isInteger(String dataType) {
    dataType = dataType.toUpperCase();
    switch (dataType) {
    case "INT":
    case "INTEGER":
    case "SMALLINT":
    case "TINYINT":
    case "SERIAL":
    case "SMALLSERIAL":
      return true;
    case "BIGINT":
      return false;
    default:
      if (dataType.contains("INT")) {
        return true;
      }
      return false;
    }
  }

  public boolean isBigInteger(String dataType) {
    switch (dataType.toUpperCase()) {
    case "BIGINT":
    case "BIGSERIAL":
      return true;
    default:
      return false;
    }
  }

  public boolean isBoolean(String dataType) {
    switch (dataType.toUpperCase()) {
    case "BOOLEAN":
    case "BOOL":
      return true;
    default:
      return false;
    }
  }

  public boolean isDouble(String dataType) {
    switch (dataType.toUpperCase()) {
    case "NUMBER":
    case "NUMERIC":
    case "DOUBLE":
    case "DECIMAL":
    case "FLOAT":
    case "REAL":
      return true;
    default:
      if (dataType.toUpperCase().contains("FLOAT")) {
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean isNumber(String dataType) {
    return isInteger(dataType) || isBigInteger(dataType) || isDouble(dataType);
  }

  public boolean isDate(String dataType) {
    switch (dataType.toUpperCase()) {
    case "DATE":
      return true;
    default:
      return false;
    }
  }

  public boolean isDateTime(String dataType) {
    dataType = dataType.toUpperCase();
    switch (dataType) {
    case "DATETIME":
    case "DATETIME2":
    case "TIMESTAMP(3)":
    case "TIMESTAMP(6)":
    case "TIMESTAMP":
    case "TIMESTAMP_WITH_TIMEZONE":
      return true;
    default:
      if (dataType.contains("DATE") || dataType.contains("TIMESTAMP")) {
        return true;
      }
      return false;
    }
  }
}
