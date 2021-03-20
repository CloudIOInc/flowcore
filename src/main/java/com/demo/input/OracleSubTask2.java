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

package com.demo.input;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.demo.events.CloudIOException;
import com.demo.events.Notification;
import com.demo.events.Producer;
import com.demo.events.StringUtil;

public class OracleSubTask2 implements Runnable {
  static Logger logger = LogManager.getLogger(OracleSubTask2.class);

  TopicPartition topicPartition;
  List<Record> list;
  private CompletableFuture<Boolean> future;
  protected boolean failed = false;
  DataSource ds;

  public OracleSubTask2(TopicPartition topicPartition, List<Record> list, DataSource ds) {
    this.list = list;
    this.topicPartition = topicPartition;
    this.ds = ds;
  }

  public void processMessages(TopicPartition topicPartition, List<Record> list) throws Throwable {
    int size = list.size();
    logger.debug("Started processing message from - {}", topicPartition.topic());
    if (list.size() == 0) {
      logger.debug("Skipped {} dumped events", size);
      return;
    }

    if (size != list.size()) {
      logger.debug("Skipped {} of {} dumped events", size - list.size(), size);
    }
    String dataTopic = "dt-013";
    try (Producer producer = Producer.get()) {
      try {
        process(producer, dataTopic, list);
        Notification.error("test", "Output",
            StringUtil.format("Failed to post {} event to {}", "", ""),
            null);

      } catch (Throwable e) {
        producer.abortTransactionQuietly();
        if (e instanceof CloudIOException) {
          ((CloudIOException) e).setErrorType("Output");
          logger.catching(e);
          //

          if (list.size() > 0) {
            list.forEach(m -> {
              //   IO.handleError(topicPartition.topic(), m, e);
            });
            Producer.sendMessages("errors", list);
          }
          //connection errors
          //        if (e instanceof PoolInitializationException || e instanceof SQLTransientConnectionException) {
          //          OutputErrors.setConnectionError(flow, flowRuntime, ErrorHandler.getMessageHTML("", e), foundDataEvent);
          //        }
          //        String errorType = isReconcileRun ? "Reconcile Output Error" : "Output Error";
          //        Notification.error(flow.getName() + "-" + flow.getUuid(), errorType,
          //            StringUtil.format("Failed to post event to {}", flow.getOutputType()),
          //            ErrorHandler.getMessageHTML("", e));

        }
        return;
      }
    }
    logger.error("No service found for output type {}!");
  }

  public void process(Producer producer, String topicName, List<Record> recs) throws Exception {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    Connection conn = null;
    Record currentRecord = null;
    try {
      for (Record r : recs) {
        producer.beginTransaction();
        currentRecord = r;
        StringBuilder query = new StringBuilder();
        query.append("SELECT z.* FROM (SELECT y.*, ROWNUM r_n FROM (");
        query.append("SELECT * FROM ").append(r.getAsString("objectName"));
        query.append(") y WHERE ROWNUM <= ?) z WHERE r_n > ? ORDER BY r_n");
        conn = ds.getConnection();
        stmt = conn.prepareStatement(query.toString());
        stmt.setObject(1, r.get("fetchCount"));
        stmt.setObject(2, r.get("offset"));
        rs = stmt.executeQuery();
        logger.info("sql query - {}, params - {} ", query.toString(), r);

        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
          int numColumns = rsmd.getColumnCount();
          Record d = new Record();
          for (int i = 1; i <= numColumns; i++) {
            String column_name = rsmd.getColumnName(i);
            Object val = rs.getObject(column_name);
            if (val instanceof Double) {
              d.setDouble(column_name, (Double) val);
            } else if (val instanceof BigDecimal) {
              BigDecimal bd = (BigDecimal) val;
              d.setBigDecimal(column_name, bd);
            } else if (val instanceof Timestamp) {
              Timestamp t = (Timestamp) val;
              r.setTimestamp(column_name, t);
            } else {
              d.setString(column_name, (String) val);
            }
          }
          producer.send(topicName, d);
          logger.info("Message - {}" + d.toString());
        }
        producer.commitTransaction();
      }
    } catch (Exception e) {
      logger.catching(e);
      throw new CloudIOException("Error while processing data - {}", currentRecord.toString());
    } finally {
      try {

        if (stmt != null)
          stmt.close();
        if (rs != null)
          rs.close();
        if (conn != null)
          conn.close();

      } catch (SQLException sqlEx) {

      }
    }

  }

  private boolean execute() throws Throwable {
    processMessages(topicPartition, list);
    return true;
  }

  @Override
  public void run() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Running task: {}", topicPartition.toString());
      }
      boolean status = execute();
      if (logger.isDebugEnabled()) {
        logger.debug("Done running task: {}", topicPartition.toString());
      }
      future.complete(status);
    } catch (Throwable e) {
      failed = true;
      logger.error("Error running task: {}", topicPartition.toString());
      logger.catching(e);
      future.completeExceptionally(e);
    }
  }

  public CompletableFuture<Boolean> start() {
    if (logger.isDebugEnabled()) {
      logger.debug("Starting task: {}", topicPartition.toString());
    }
    future = new CompletableFuture<Boolean>();
    IO.execute(this);
    return future;
  }
}
