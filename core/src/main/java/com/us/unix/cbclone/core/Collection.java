package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Collection {
  public String name;
  public int ttl;
  public boolean history;
  private final ObjectMapper mapper = new ObjectMapper();

  public Collection(String name, int ttl, boolean history) {
    this.name = name;
    this.ttl = ttl;
    this.history = history;
  }

  public Collection(String name, int ttl) {
    this.name = name;
    this.ttl = ttl;
    this.history = false;
  }

  public Collection(String name) {
    this.name = name;
    this.ttl = 0;
    this.history = false;
  }

  public Collection(JsonNode data) {
    this.name = data.get("name").asText();
    this.ttl = data.get("maxTTL").asInt();
    this.history = data.get("history").asBoolean();
  }

  public JsonNode toJson() {
    ObjectNode node = mapper.createObjectNode();
    node.put("name", name);
    node.put("maxTTL", ttl);
    node.put("history", history);
    return node;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }
}
