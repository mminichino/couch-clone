package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Column {
  public String name;
  public DataType type;
  public boolean isKey;
  private final ObjectMapper mapper = new ObjectMapper();

  public Column(String name, DataType type, boolean isKey) {
    this.name = name;
    this.type = type;
    this.isKey = isKey;
  }

  public Column(String name, DataType type) {
    this.name = name;
    this.type = type;
    this.isKey = false;
  }

  public Column(JsonNode data) {
    this.name = data.get("name").asText();
    this.type = DataType.valueOf(data.get("type").asText());
    this.isKey = data.get("isKey").asBoolean();
  }

  public JsonNode toJson() {
    ObjectNode node = mapper.createObjectNode();
    node.put("name", name);
    node.put("type", type.toString());
    node.put("isKey", isKey);
    return node;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }
}
