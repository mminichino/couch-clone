package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Table {
  public String name;
  public long size;
  public long rows;
  public TableType type;
  public JsonNode bucket;
  public List<Column> columns;
  public String password;
  public String scope = "_default";
  public String collection = "_default";
  public int ttl = 0;
  private final ObjectMapper mapper = new ObjectMapper();

  public Table(String name, long size, List<Column> columns) {
    this.name = name;
    this.size = size;
    this.rows = 0L;
    this.type = TableType.RDBMS;
    this.columns = columns;
    this.bucket = mapper.createObjectNode();
  }

  public Table(String name, long size, TableType type) {
    this.name = name;
    this.size = size;
    this.rows = 0L;
    this.type = type;
    this.columns = new ArrayList<>();
    this.bucket = mapper.createObjectNode();
  }

  public Table(String name, long size, TableType type, String scope) {
    this.name = name;
    this.size = size;
    this.rows = 0L;
    this.type = type;
    this.columns = new ArrayList<>();
    this.bucket = mapper.createObjectNode();
    this.scope = scope;
  }

  public Table(JsonNode bucketJson, String scope, String collection) {
    String name = bucketJson.get("name").asText();
    String type = bucketJson.get("bucketType").asText();
    int quota = bucketJson.get("quota").get("ram").asInt() / 1048576;
    int replicas = bucketJson.get("replicaNumber").asInt();
    String eviction = bucketJson.get("evictionPolicy").asText();
    int ttl = bucketJson.has("maxTTL") ? bucketJson.get("maxTTL").asInt() : 0;
    String storage = bucketJson.has("storageBackend") ? bucketJson.get("storageBackend").asText() : "couchstore";
    String resolution = bucketJson.has("conflictResolutionType") ? bucketJson.get("conflictResolutionType").asText() : "seqno";
    String password = bucketJson.has("saslPassword") ? bucketJson.get("saslPassword").asText() : "";

    ObjectNode bucket = mapper.createObjectNode();
    bucket.put("name", name);
    bucket.put("type", type);
    bucket.put("quota", quota);
    bucket.put("replicas", replicas);
    bucket.put("eviction", eviction);
    bucket.put("ttl", ttl);
    bucket.put("storage", storage);
    bucket.put("resolution", resolution);
    bucket.put("password", password);

    this.name = collection.equals("_default") ? name : name + "." + scope + "." + collection;
    this.size = quota;
    this.rows = 0L;
    this.type = TableType.COUCHBASE;
    this.columns = new ArrayList<>();
    this.bucket = bucket;
    this.scope = scope;
    this.collection = collection;
    this.ttl = ttl;
    this.password = password;
  }

  public Table(JsonNode data) {
    this.name = data.get("name").asText();
    this.size = data.get("size").asLong();
    this.rows = data.get("rows").asLong();
    this.type = TableType.valueOf(data.get("type").asText());
    this.bucket = data.get("bucket");
    this.columns = getColumnList(data.get("columns"));
    this.scope = data.get("scope").asText();
    this.collection = data.get("collection").asText();
    this.ttl = data.get("ttl").asInt();
    this.password = data.get("password").asText();
  }

  public static Table inList(List<Table> tables, String name) {
    for (Table t : tables) {
      if (t.name.equals(name)) {
        return t;
      }
    }
    return null;
  }

  private List<Column> getColumnList(JsonNode data) {
    Iterator<JsonNode> elements = data.elements();
    List<Column> items = new ArrayList<>();
    while(elements.hasNext()){
      items.add(new Column(elements.next()));
    }
    return items;
  }

  private ArrayNode getColumnArray(List<Column> columns) {
    ArrayNode array = mapper.createArrayNode();
    for (Column c : columns) {
      array.add(c.toJson());
    }
    return array;
  }

  public JsonNode toJson() {
    ObjectNode table = mapper.createObjectNode();
    table.put("name", name);
    table.put("size", size);
    table.put("rows", rows);
    table.put("type", type.toString());
    table.set("bucket", bucket);
    table.set("columns", getColumnArray(columns));
    table.put("scope", scope);
    table.put("collection", collection);
    table.put("ttl", ttl);
    table.put("password", password);
    return table;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }
}
