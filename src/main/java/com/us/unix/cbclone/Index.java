package com.us.unix.cbclone;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Index {
  public String column;
  public String table;
  public String name;
  public String condition;
  public boolean isPrimary = false;

  public Index(String column, String table, String name, String condition) {
    this.column = column.replace("`", "");
    this.table = table;
    this.name = name;
    this.condition = condition;
  }

  public Index(String column, String table, String name) {
    this.column = column.replace("`", "");
    this.table = table;
    this.name = name;
    this.condition = "";
  }

  public Index(String column, String table) {
    this.column = column.replace("`", "");
    this.table = table;
    this.name = this.column + "_idx";
    this.condition = "";
  }

  public Index(String table) {
    this.column = "#primary";
    this.table = table;
    this.name = this.table + "_primary_idx";
    this.condition = "";
    this.isPrimary = true;
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

  public boolean isPrimary() {
    return isPrimary;
  }

  public JsonNode json() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode data = mapper.createObjectNode();
    data.put("column", column);
    data.put("table", table);
    data.put("name", name);
    data.put("condition", condition);
    data.put("isPrimary", isPrimary);
    return data;
  }
}
