
package io.cloudio.messages;

public class DBSettings extends Settings {

  private String jdbcUrl;
  private String userName;
  private String password;

  private String tableName;
  private String lastUpdateColumn;
  private String idColumn;

  private int fetchSize;

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getLastUpdateColumn() {
    return lastUpdateColumn;
  }

  public void setLastUpdateColumn(String lastUpdateColumn) {
    this.lastUpdateColumn = lastUpdateColumn;
  }

  public String getIdColumn() {
    return idColumn;
  }

  public void setIdColumn(String idColumn) {
    this.idColumn = idColumn;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

}
