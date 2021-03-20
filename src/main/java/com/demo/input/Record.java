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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.demo.events.CloudIOException;
import com.demo.util.JsonUtils;
import com.demo.util.Util;
import com.demo.util.Util.RecordStatus;
import com.google.gson.JsonObject;

public class Record extends TreeMap<String, Object> {
  public static final Record EMPTY_RECORD = Record.createForKafka();
  static Logger logger = LogManager.getLogger(Record.class);
  private static final long serialVersionUID = 1L;
  static {
    EMPTY_RECORD.setReadOnly(true);
  }

  public static final Record create() {
    return new Record();
  }

  public static final Record createForKafka() {
    return createForKafka(null);
  }

  public static final Record createForKafka(Map<String, Object> map) {
    Record r = map == null ? new Record() : new Record(map);
    r.setUpdateWhoColumns(false);
    r.setTrackDirtyState(false);
    return r;
  }

  public static Record createLikeMap() {
    Record r = new Record();
    r.setNormalizeValues(false);
    r.setUpdateWhoColumns(false);
    r.setTrackDirtyState(false);
    return r;
  }

  public static Record createLikeMap(Map<String, Object> map) {
    Record r = createLikeMap();
    r.putAll(map);
    return r;
  }

  public static final Record emptyRecord() {
    return EMPTY_RECORD;
  }

  public static final boolean equalWithNull(Object obj1, Object obj2) {
    if (obj1 == obj2) {
      return true;
    } else if (obj1 == null) {
      return false;
    } else {
      if (obj1 instanceof Number && obj2 instanceof Number) {
        return ((Number) obj1).doubleValue() == ((Number) obj2).doubleValue();
      }
      return obj1.equals(obj2);
    }
  }

  public static Record of(Object... kv) {
    Record record = Record.create();
    return record.fill(kv);
  }

  public static final String valueToString(Object value) {
    if (value == null) {
      return "";
    }
    if (value instanceof String) {
      return (String) value;
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    } else if (value instanceof Double) {
      return ((Double) value).doubleValue() + "";
    } else if (value instanceof Number) {
      return ((Number) value).longValue() + "";
    }
    if (value instanceof Date) {
      return JsonUtils.dateToJsonString((Date) value);
    }
    return value.toString();
  }

  private transient Map<String, Object> data;
  private boolean markAllSetterCallsDirty = false;
  private transient boolean normalizeValues = true;
  private transient Record origValues;
  private final AtomicBoolean readOnly = new AtomicBoolean(false);
  public transient ConsumerRecord<?, ?> record;
  private HashMap<String, List<Record>> recordListMap;
  private RecordStatus recordStatus = RecordStatus.New;
  private transient boolean skipDML = false;
  private boolean trackDirtyState = false;
  private boolean updateWhoColumns = true;

  protected Record() {

  }

  public Record(Map<String, Object> map) {
    this.putAll(map);
  }

  public Set<java.util.Map.Entry<String, List<Record>>> childEntrySet() {
    if (recordListMap == null) {
      return Collections.emptySet();
    }
    return recordListMap.entrySet();
  }

  @Override
  public Record clone() {
    Record r = new Record();
    copyDataTo(r);
    return r;
  }

  public void commit() {
    this.origValues = null;
    this.setTrackDirtyState(true);
    if (this.recordStatus != Util.RecordStatus.Delete) {
      this.recordStatus = Util.RecordStatus.Query;
    }
  }

  public void copyDataTo(Record r) {
    r.normalizeValues = normalizeValues;
    r.setTrackDirtyState(false); // avoid tracking changes during copy
    r.putAll(this);
    r.updateWhoColumns = this.updateWhoColumns;
    r.recordStatus = this.recordStatus;
    if (recordListMap != null) {
      r.recordListMap = new HashMap<>(recordListMap);
    }
    r.setTrackDirtyState(isTrackDirtyState());
    r.markAllSetterCallsDirty = isMarkAllSetterCallsDirty();
    if (data != null) {
      r.data = new HashMap<>(data);
    }
    r.origValues = origValues;
    r.readOnly.set(readOnly.get());
  }

  private void ensureRecordList() {
    recordListMap = new HashMap<>();
  }

  public <T extends Record> T fill(Object... kv) {
    for (int i = 0; i < kv.length; i += 2) {
      put((String) kv[i], kv[i + 1]);
    }
    setRecordStatus(Util.RecordStatus.Insert);
    return (T) this;
  }

  public boolean filter(Record filters) {
    boolean found = false;
    for (String key : filters.keySet()) {
      if (isSpecialKey(key)) {
        if (!hasSpecialKeyMatched(key, filters.getString(key))) {
          return false;
        }
        found = true;
        continue;
      } else {
        found = filter(key, filters.getString(key));
        if (!found) {
          return false;
        }
      }
    }
    return found;
  }

  public boolean isSpecialKey(String key) {
    switch (key) {
    case "$offset":
    case "$offset_lte":
    case "$key":
      return true;
    default:
      return false;
    }
  }

  public boolean hasSpecialKeyMatched(String filterAttribute, String filterValue) {
    switch (filterAttribute) {
    case "$offset":
      try {
        long offset = Long.parseLong(filterValue);
        if (record.offset() == offset) {
          return true;
        }
      } catch (Exception e) {
        // ignore me
      }
      break;
    case "$offset_lte":
      try {
        long offset = Long.parseLong(filterValue);
        if (record.offset() <= offset) {
          return true;
        }
      } catch (Exception e) {
        // ignore me
      }
      break;
    case "$key":
      Object key = record.key();
      return key != null && key.toString().toLowerCase().contains(filterValue);
    }
    return false;
  }

  public boolean filter(String filterAttribute, String filterValue) {
    boolean found = false;
    for (Entry<String, Object> entry : super.entrySet()) {
      Object value = entry.getValue();
      if (value == null) {
        continue;
      }

      if ("all".equals(filterAttribute) || entry.getKey().equals(filterAttribute) || value instanceof Record) {
        if (value instanceof List && ((List) value).size() > 0 && ((List) value).get(0) instanceof Record) {
          for (Record r : ((List<Record>) value)) {
            found = r.filter(filterAttribute, filterValue);
            if (found) {
              break;
            }
          }
        } else if (value instanceof Record) {
          found = ((Record) value).filter(filterAttribute, filterValue);
        } else if (value instanceof String) {
          found = ((String) value).toLowerCase().contains(filterValue);
        } else if (value instanceof Number) {
          found = value.toString().equals(filterValue);
        } else {
          found = value.toString().toLowerCase().contains(filterValue);
        }

        if (found) {
          return true;
        }
      }
    }
    return false;
  }

  public final String getAsString(String attributeCode) {
    return valueToString(get(attributeCode));
  }

  public final BigDecimal getBigDecimal(String attributeCode) {
    return getBigDecimal(attributeCode, false);
  }

  public final BigDecimal getBigDecimal(String attributeCode, boolean recursive) {
    Object value = recursive ? getRecursive(attributeCode) : get(attributeCode);
    if (value != null) {
      return value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
    }
    return null;
  }

  public final Integer getBit(String attributeCode) {
    Object value = get(attributeCode);
    if (value == null) {
      return null;
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof Boolean) {
      return ((Boolean) value) ? 1 : 0;
    }
    try {
      return new Integer(value.toString());
    } catch (NumberFormatException e) {
      throw new CloudIOException("Invalid number",
          "Invalid number " + value.toString() + " for attribute " + attributeCode);
    }
  }

  public Boolean getBoolean(String attributeCode) {
    Object value = get(attributeCode);
    if (value != null) {
      return value instanceof Boolean ? (Boolean) value : Boolean.valueOf(value.toString());
    }
    return null;
  }

  public final Timestamp getCreationDate() {
    return (Timestamp) get("creationDate");
  }

  public final Date getDate(String attributeCode) {
    Object d = get(attributeCode);
    if (d instanceof Date) {
      return (Date) d;
    } else if (d == null) {
      return null;
    } else {
      return JsonUtils.toTimestamp6(d.toString());
    }
  }

  public final Double getDouble(String attributeCode) {
    Number value = getNumber(attributeCode);
    if (value != null) {
      return value instanceof Double ? (Double) value : value.doubleValue();
    }
    return null;
  }

  public final Integer getInteger(String attributeCode) {
    Number value = getNumber(attributeCode);
    if (value != null) {
      return value instanceof Integer ? (Integer) value : value.intValue();
    }
    return null;
  }

  public final Timestamp getLastUpdateDate() {
    return (Timestamp) get("lastUpdateDate");
  }

  public final Long getLong(String attributeCode) {
    Number value = getNumber(attributeCode);
    if (value != null) {
      return value instanceof Long ? (Long) value : value.longValue();
    }
    return null;
  }

  public final Number getNumber(String attributeCode) {
    Object value = get(attributeCode);
    if (value == null || "".equals(value)) {
      return null;
    } else if (value instanceof Number) {
      return (Number) value;
    }
    try {
      return new BigDecimal(value.toString());
    } catch (NumberFormatException e) {
      throw new CloudIOException("Invalid number",
          "Invalid number " + value.toString() + " for attribute " + attributeCode);
    }
  }

  public Record getOriginalValues() {
    return this.origValues;
  }

  public final Record getRecord(String attributeCode) {
    Object value = get(attributeCode);
    if (value == null) {
      return Record.createForKafka();
    }
    if (value instanceof Record) {
      return (Record) value;
    } else {
      throw CloudIOException.with("Invalid object for attribute {}. Excepted object but found {}", attributeCode,
          value.getClass().getSimpleName());
    }
  }

  @SuppressWarnings("unchecked")
  public final List<Record> getRecordList(String attributeCode) {
    Object value = get(attributeCode);
    if (value == null) {
      return Collections.emptyList();
    }
    if (value instanceof List) {
      return (List<Record>) value;
    } else {
      throw CloudIOException.with("Invalid array for attribute {}. Excepted array but found {}", attributeCode,
          value.getClass().getSimpleName());
    }
  }

  @SuppressWarnings("unchecked")
  public final ArrayList<Record> getRecordArray(String attributeCode) {
    Object value = get(attributeCode);
    if (value == null) {
      return null;
    }
    if (value instanceof ArrayList) {
      return (ArrayList<Record>) value;
    } else {
      throw CloudIOException.with("Invalid array for attribute {}. Excepted array but found {}", attributeCode,
          value.getClass().getSimpleName());
    }
  }

  public final RecordStatus getRecordStatus() {
    return recordStatus;
  }

  public final Object getRecursive(String attributeCode) {
    String[] attrs = StringUtils.split(attributeCode, '.');
    Object value = get(attrs[0]);
    if (value == null) {
      return null;
    }
    if (attrs.length > 1) {
      if (value instanceof Record) {
        return ((Record) value).getRecursive(attributeCode.substring(attrs[0].length() + 1));
      } else {
        return null;
      }
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T getSessionData(String key) {
    if (data == null) {
      return null;
    }
    return (T) data.get(key);
  }

  public final String getString(String attributeCode) {
    Object value = get(attributeCode);
    if (value != null) {
      return value.toString();
    }
    return null;
  }

  public final String getString(String attributeCode, String defaultValue) {
    String value = getString(attributeCode);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  public final Timestamp getTimestamp(String attributeCode) {
    return getTimestamp(attributeCode, false);
  }

  public final Timestamp getTimestamp(String attributeCode, boolean recursive) {
    Object value = recursive ? getRecursive(attributeCode) : get(attributeCode);
    if (value == null) {
      return null;
    }
    if (value instanceof Timestamp) {
      return (Timestamp) value;
    } else if (value instanceof Date) {
      return new Timestamp(((Date) value).getTime());
    }
    return JsonUtils.toTimestamp6(value.toString());
  }

  public final boolean has(String attributeCode) {
    return containsKey(attributeCode);
  }

  public boolean hasChildren() {
    return recordListMap != null;
  }

  public final boolean is(String attributeCode) {
    return isTrue(attributeCode);
  }

  public final boolean isDelete() {
    return recordStatus == RecordStatus.Delete;
  }

  public final boolean isDirty() {
    return recordStatus == RecordStatus.Update || recordStatus == RecordStatus.Insert;
  }

  public final boolean isFromDB() {
    return !(isInsert() || isNew()
        || (getRecordStatus() == RecordStatus.Validate
            && (getRecordStatus() == RecordStatus.New || getRecordStatus() == RecordStatus.Insert)));
  }

  public final boolean isInsert() {
    return recordStatus == RecordStatus.Insert;
  }

  public boolean isMarkAllSetterCallsDirty() {
    return markAllSetterCallsDirty;
  }

  public final boolean isNew() {
    return recordStatus == RecordStatus.New;
  }

  public boolean isNormalizeValues() {
    return normalizeValues;
  }

  public final boolean isQuery() {
    return recordStatus == RecordStatus.Query;
  }

  public boolean isReadOnly() {
    return readOnly.get();
  }

  public final boolean isRecord(String attributeCode) {
    Object value = get(attributeCode);
    if (value instanceof Record) {
      return true;
    }
    return false;
  }

  public boolean isSkipDML() {
    return skipDML;
  }

  public final boolean isSync() {
    return recordStatus == RecordStatus.Sync;
  }

  public boolean isTrackDirtyState() {
    return trackDirtyState;
  }

  public final boolean isTrue(String attributeCode) {
    Object value = get(attributeCode);
    if (value instanceof Boolean) {
      return ((Boolean) value).booleanValue();
    }
    return false;
  }

  public final boolean isUpdate() {
    return recordStatus == RecordStatus.Update;
  }

  public boolean isUpdateWhoColumns() {
    return updateWhoColumns;
  }

  public boolean isUpsert() {
    return recordStatus == RecordStatus.Upsert;
  }

  public Object nestedRemove(String path) {
    int index = path.indexOf(".");
    if (index == -1) {
      return remove(path);
    }
    String key = path.substring(0, index);
    Object child = get(key);
    if (child instanceof Record) {
      return ((Record) child).nestedRemove(path.substring(index));
    }
    if (child instanceof List) {
      for (Object c : ((List) child)) {
        if (c instanceof Record) {
          ((Record) child).nestedRemove(path.substring(index));
        }
      }
    }
    return remove(key);
  }

  private Object normalizeValue(Object value) {
    if (!normalizeValues) {
      return value;
    }
    if (value == null
        || value instanceof String
        || value instanceof BigDecimal
        || value instanceof Double
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Timestamp
        || value instanceof Boolean
        || value instanceof List
        || value instanceof Map) {
      return value;
    }
    if (value instanceof Number) {
      return BigDecimal.valueOf(((Number) value).doubleValue());
    }
    if (value instanceof Instant) {
      return Timestamp.from((Instant) value);
    }
    if (value instanceof Date) {
      return new Timestamp(((Date) value).getTime());
    }
    return value.toString();
  }

  private final void onChange(String key, Object oldValue, Object newValue) {
    if (!isTrackDirtyState()) {
      return;
    }
    if (isMarkAllSetterCallsDirty() || !equalWithNull(newValue, oldValue)) {
      if (origValues == null) {
        origValues = Record.createLikeMap();
      }
      if (!origValues.containsKey(key)) {
        origValues.put(key, oldValue);
      }
      this.setDirty();
    }
  }

  public Record pick(String... keys) {
    Record r = Record.createLikeMap();
    for (String key : keys) {
      if (has(key)) {
        r.put(key, get(key));
      }
    }
    return r;
  }

  public Record omit(String... keys) {
    Record r = Record.createForKafka(this);
    for (String key : keys) {
      r.remove(key);
    }
    return r;
  }

  @Override
  public Object put(String key, Object value) {
    if (isReadOnly()) {
      throw CloudIOException.with("Record is readonly! Cannot update {} with {}!", key, value);
    }

    // do not trim spaces, as some source system may have space as a value (intentional)

    //    if (value instanceof String) {
    //      String str = (String) value;
    //      str = str.trim();
    //      if (str.length() == 0) {
    //        str = null;
    //      }
    //      value = str;
    //    }

    Object oldValue = null;
    synchronized (this) {
      oldValue = super.put(key, normalizeValue(value));
      onChange(key, oldValue, value);
    }
    return oldValue;
  }

  protected void superPut(String key, Object value) {
    super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends Object> m) {
    synchronized (m) {
      for (Entry<? extends String, ? extends Object> e : m.entrySet()) {
        put(e.getKey(), e.getValue());
      }
    }
  }

  public void removeOriginalValue(String key) {
    if (origValues != null) {
      origValues.remove(key);
      if (origValues.isEmpty()) {
        origValues = null;
      }
    }
  }

  public void useOriginalValues(Record origValues) {
    this.origValues = origValues;
  }

  public Object set(String key, Object value) {
    return put(key, value);
  }

  public BigDecimal setBigDecimal(String key, BigDecimal value) {
    Object oldValue = put(key, value);
    if (oldValue == null || oldValue instanceof BigDecimal) {
      return (BigDecimal) oldValue;
    }
    if (oldValue instanceof Number) {
      return BigDecimal.valueOf(((Number) oldValue).doubleValue());
    }
    return null;
  }

  public Boolean setBoolean(String key, Boolean value) {
    return (Boolean) put(key, value);
  }

  public void setChildren(String attributeCode, List<Record> value) {
    ensureRecordList();
    recordListMap.put(attributeCode, value);
  }

  public final Timestamp setCreationDate(Timestamp date) {
    return setTimestamp("creationDate", date);
  }

  public Timestamp setDate(String key, Date value) {
    return setTimestamp(key, new Timestamp(value.getTime()));
  }

  public final void setDirty() {
    if (this.isNew()) {
      this.setRecordStatus(Util.RecordStatus.Insert);
    } else if (this.isQuery()) {
      this.setRecordStatus(Util.RecordStatus.Update);
    }
  }

  public Double setDouble(String key, Double value) {
    Object oldValue = put(key, value);
    if (oldValue == null || oldValue instanceof Double) {
      return (Double) oldValue;
    }
    if (oldValue instanceof Number) {
      return ((Number) oldValue).doubleValue();
    }
    return null;
  }

  public Integer setInteger(String key, Integer value) {
    Object oldValue = put(key, value);
    if (oldValue == null || oldValue instanceof Integer) {
      return (Integer) oldValue;
    }
    if (oldValue instanceof Number) {
      return ((Number) oldValue).intValue();
    }
    return null;
  }

  public final Timestamp setLastUpdateDate(Timestamp date) {
    return setTimestamp("lastUpdateDate", date);
  }

  public Long setLong(String key, Long value) {
    Object oldValue = put(key, value);
    if (oldValue == null || oldValue instanceof Long) {
      return (Long) oldValue;
    }
    if (oldValue instanceof Number) {
      return ((Number) oldValue).longValue();
    }
    return null;
  }

  public void setMarkAllSetterCallsDirty(boolean markAllSetterCallsDirty) {
    this.markAllSetterCallsDirty = markAllSetterCallsDirty;
  }

  public void setNormalizeValues(boolean normalizeValues) {
    this.normalizeValues = normalizeValues;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly.set(readOnly);
  }

  public final void setRecordStatus(RecordStatus recordStatus) {
    // logger.info("Changing record status of {}  from {} to {}", this.getString("uuid"), this.recordStatus, recordStatus);
    this.recordStatus = recordStatus;
  }

  public void setSessionData(String key, Object value) {
    if (data == null) {
      data = new HashMap<>();
    }
    data.put(key, value);
  }

  public void setSkipDML(boolean skipDML) {
    this.skipDML = skipDML;
  }

  public final String setString(String attributeCode, String value) {
    Object oldValue = set(attributeCode, value);
    if (oldValue != null && !(oldValue instanceof String)) {
      // must be a json object i.e. Record
      return oldValue.toString();
    }
    return (String) oldValue;
  }

  public final Timestamp setTimestamp(String attributeCode, Timestamp value) {
    Timestamp oldValue = getTimestamp(attributeCode);
    put(attributeCode, value);
    return oldValue;
  }

  public void setTrackDirtyState(boolean trackDirtyState) {
    this.trackDirtyState = trackDirtyState;
  }

  public void setUpdateWhoColumns(boolean updateWhoColumns) {
    this.updateWhoColumns = updateWhoColumns;
  }

  public final JsonObject toJSON() {
    JsonObject json = new JsonObject();
    for (String key : keySet()) {
      Object d = get(key);
      if (d == null) {
        json.add(key, null);
      } else if (d instanceof Date) {
        json.addProperty(key, JsonUtils.dateToJsonString((Date) d));
      } else if (d instanceof Number) {
        json.addProperty(key, (Number) d);
      } else {
        json.addProperty(key, d.toString());
      }
    }
    return json;
  }

  @Override
  public final String toString() {
    return KafkaUtil.getSerializer().toJson(this);
  }

  public Record with(Record m) {
    this.putAll(m);
    this.setRecordStatus(m.getRecordStatus());
    return this;
  }

}
