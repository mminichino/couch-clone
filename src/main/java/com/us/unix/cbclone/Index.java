package com.us.unix.cbclone;

public class Index {
  public String column;
  public String table;
  public String name;
  public String condition;

  public Index(String column, String table, String name, String condition) {
    this.column = column;
    this.table = table;
    this.name = name;
    this.condition = condition;
  }

  public Index(String column, String table, String name) {
    this.column = column;
    this.table = table;
    this.name = name;
    this.condition = "";
  }

  public Index(String column, String table) {
    this.column = column;
    this.table = table;
    this.name = this.column + "_idx";
    this.condition = "";
  }

  public String getColumn() {
    return column;
  }

  public String getTable() {
    return table;
  }

  public String getName() {
    return name;
  }

  public String getCondition() {
    return condition;
  }
}
