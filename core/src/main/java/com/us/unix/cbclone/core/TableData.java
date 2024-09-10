package com.us.unix.cbclone.core;

import java.util.List;

public class TableData {
  private String name;
  private long size;
  private long rows;
  private TableType type;
  private BucketData bucket;
  private List<ColumnData> columns;
  private String password;
  private ScopeData scope;
  private CollectionData collection;
  private List<IndexData> indexes;
  private List<SearchIndexData> searchIndexes;

  public String getName() {
    return name;
  }

  public long getSize() {
    return size;
  }

  public long getRows() {
    return rows;
  }

  public TableType getType() {
    return type;
  }

  public BucketData getBucket() {
    return bucket;
  }

  public List<ColumnData> getColumns() {
    return columns;
  }

  public String getPassword() {
    return password;
  }

  public ScopeData getScope() {
    return scope;
  }

  public CollectionData getCollection() {
    return collection;
  }

  public List<IndexData> getIndexes() {
    return indexes;
  }

  public List<SearchIndexData> getSearchIndexes() {
    return searchIndexes;
  }

  public void setName(String name) {
    this.name = name;
  }


  public void setSize(long size) {
    this.size = size;
  }

  public void setRows(long rows) {
    this.rows = rows;
  }

  public void setType(TableType type) {
    this.type = type;
  }

  public void setBucket(BucketData bucket) {
    this.bucket = bucket;
  }

  public void setColumns(List<ColumnData> columns) {
    this.columns = columns;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setScope(ScopeData scope) {
    this.scope = scope;
  }

  public void setCollection(CollectionData collection) {
    this.collection = collection;
  }

  public void setIndexes(List<IndexData> indexes) {
    this.indexes = indexes;
  }

  public void setSearchIndexes(List<SearchIndexData> searchIndexes) {
    this.searchIndexes = searchIndexes;
  }

  public static TableData inList(List<TableData> tables, String name) {
    for (TableData t : tables) {
      if (t.getName().equals(name)) {
        return t;
      }
    }
    return null;
  }
}
