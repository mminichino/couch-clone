package com.us.unix.cbclone.core;

import java.util.List;

public class IndexData {
  private String column;
  private String table;
  private String name;
  private List<String> indexKeys;
  private String condition;
  private boolean isPrimary;

  public String getColumn() {
    return column;
  }

  public String getTable() {
    return table;
  }

  public String getName() {
    return name;
  }

  public List<String> getIndexKeys() {
    return indexKeys;
  }

  public String getCondition() {
    return condition;
  }

  public boolean isPrimary() {
    return isPrimary;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setIndexKeys(List<String> indexKeys) {
    this.indexKeys = indexKeys;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public void setPrimary(boolean isPrimary) {
    this.isPrimary = isPrimary;
  }
}
