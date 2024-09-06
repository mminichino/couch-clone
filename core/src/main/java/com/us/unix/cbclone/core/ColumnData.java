package com.us.unix.cbclone.core;

public class ColumnData {
  private String name;
  private DataType type;
  private boolean isKey;

  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  public boolean isKey() {
    return isKey;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(DataType type) {
    this.type = type;
  }

  public void setKey(boolean isKey) {
    this.isKey = isKey;
  }
}
