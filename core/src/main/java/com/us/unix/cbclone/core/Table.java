package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public class Table {
  public String name;
  public long size;
  public long rows;
  public TableType type;
  public JsonNode bucket;
  public List<Column> columns;
  public List<Scope> scopes;
  private final ObjectMapper mapper = new ObjectMapper();

  public Table(String name, long size, List<Column> columns) {
    this.name = name;
    this.size = size;
    this.rows = 0L;
    this.type = TableType.RDBMS;
    this.columns = columns;
    this.bucket = mapper.createObjectNode();
    this.scopes = new ArrayList<>();
  }

  public Table(String name, long size, TableType type) {
    this.name = name;
    this.size = size;
    this.rows = 0L;
    this.type = type;
    this.columns = new ArrayList<>();
    this.bucket = mapper.createObjectNode();
    this.scopes = new ArrayList<>();
  }

  public Table(String name, long size, TableType type, List<Scope> scopes) {
    this.name = name;
    this.size = size;
    this.rows = 0L;
    this.type = type;
    this.columns = new ArrayList<>();
    this.bucket = mapper.createObjectNode();;
    this.scopes = scopes;
  }

  public Table(JsonNode data, List<Scope> scopes) {
    String name = data.get("name").asText();
    String type = data.get("bucketType").asText();
    int quota = data.get("quota").get("ram").asInt() / 1048576;
    int replicas = data.get("replicaNumber").asInt();
    String eviction = data.get("evictionPolicy").asText();
    int ttl = data.has("maxTTL") ? data.get("maxTTL").asInt() : 0;
    String storage = data.has("storageBackend") ? data.get("storageBackend").asText() : "couchstore";
    String resolution = data.has("conflictResolutionType") ? data.get("conflictResolutionType").asText() : "seqno";

    ObjectNode bucket = mapper.createObjectNode();
    bucket.put("name", name);
    bucket.put("type", type);
    bucket.put("quota", quota);
    bucket.put("replicas", replicas);
    bucket.put("eviction", eviction);
    bucket.put("ttl", ttl);
    bucket.put("storage", storage);
    bucket.put("resolution", resolution);

    this.name = name;
    this.size = quota;
    this.rows = 0L;
    this.type = TableType.COUCHBASE;
    this.columns = new ArrayList<>();
    this.bucket = bucket;
    this.scopes = scopes;
  }
}
