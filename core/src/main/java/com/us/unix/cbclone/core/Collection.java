package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;

public class Collection {
  public String name;
  public int ttl;
  public boolean history;

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
    this.name = data.has("name") ? data.get("name").asText() : "_default";
    this.ttl = data.has("maxTTL") ? data.get("maxTTL").asInt() : 0;
    this.history = data.has("history") && data.get("history").asBoolean();
  }
}
