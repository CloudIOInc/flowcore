
package io.cloudio.messages;

import java.util.Date;

public class OracleSettings extends Settings {

  private String jdbcUrl;
  private String userName;
  private String password;

  private String tableName;
  private String lastUpdateColumn;
  private Date lud;
  private String idColumn;
  private Double idVal;

  private Integer fetchSize;
  private Integer partitionSize;

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

  public Date getLastUpdateDate() {
    return lud;
  }

  public void setLastUpdateDate(Date lud) {
    this.lud = lud;
  }

  public Double getIdVal() {
    return idVal;
  }

  public void setIdVal(Double idVal) {
    this.idVal = idVal;
  }

  public Integer getPartitionSize() {
    return partitionSize;
  }

  public void setPartitionSize(Integer partitionSize) {
    this.partitionSize = partitionSize;
  }

  public void setFetchSize(Integer fetchSize) {
    this.fetchSize = fetchSize;
  }

}
