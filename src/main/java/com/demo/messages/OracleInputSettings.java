
package com.demo.messages;

public class OracleInputSettings extends Settings {

  private String tableName;
  private String userName;
  private String password;
  private String jdbcUrl;
  private long partition;
  private long fetchSize;

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
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

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public long getPartition() {
    return partition;
  }

  public void setPartition(long partition) {
    this.partition = partition;
  }

  public long getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(long fetchSize) {
    this.fetchSize = fetchSize;
  }

}
