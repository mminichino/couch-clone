package com.us.unix.cbclone.couchbase3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.us.unix.cbclone.core.*;

import java.io.Writer;
import java.util.List;
import java.util.Properties;

public class CouchbaseDriver extends DatabaseDriver {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String BUCKET_PASSWORD = "couchbase.bucketPassword";
  public static final String LEGACY_AUTH = "couchbase.legacyAuth";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_BUCKET_PASSWORD = "";
  public static final String DEFAULT_LEGACY_AUTH = "false";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "default";
  public String hostname;
  public String username;
  public String password;
  public String bucket;
  public boolean legacyAuth;
  public String bucketPassword;
  public static volatile CouchbaseConnect db;
  public static volatile CouchbaseStream stream;

  @Override
  public void initDb(Properties properties) {
    hostname = properties.getProperty(CLUSTER_HOST, DEFAULT_HOSTNAME);
    username = properties.getProperty(CLUSTER_USER, DEFAULT_USER);
    password = properties.getProperty(CLUSTER_PASSWORD, DEFAULT_PASSWORD);
    bucketPassword = properties.getProperty(BUCKET_PASSWORD, DEFAULT_BUCKET_PASSWORD);
    legacyAuth = Boolean.parseBoolean(properties.getProperty(LEGACY_AUTH, DEFAULT_LEGACY_AUTH));

    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    db = dbBuilder
        .host(hostname)
        .username(username)
        .password(password)
        .build();
  }

  @Override
  public List<TableData> exportTables() {
    return db.getBuckets();
  }

  @Override
  public List<UserData> exportUsers() {
    return db.getUsers();
  }

  @Override
  public List<GroupData> exportGroups() {
    return db.getGroups();
  }

  @Override
  public void exportData(Writer writer, TableData table) {
    String bucketName = table.getName();
    String scopeName = table.getScope().getName();
    String collectionName = table.getCollection().getName();
    stream = db.stream(bucketName, scopeName, collectionName);
    stream.toWriter(writer);
  }

  @Override
  public void importTables(List<TableData> tables) {
    db.createBuckets(tables);
  }

  @Override
  public void importUsers(List<UserData> users) {

  }

  @Override
  public void importGroups(List<GroupData> groups) {

  }

  @Override
  public void importData(FileReader reader, String table) {
    ObjectMapper mapper = new ObjectMapper();
    String[] keyspace = table.split("\\.");
    String bucketName = keyspace[0];
    String scopeName = keyspace[1];
    String collectionName = keyspace[2];
    System.out.printf("Importing keyspace %s%n", table);
    db.connectBucket(bucketName);
    db.connectScope(scopeName);
    db.connectCollection(collectionName);
    try {
      for (String line = reader.readLine(); line != null && !line.equals("__END__"); line = reader.readLine()) {
        JsonNode node = mapper.readTree(line);
        String id = node.get("metadata").get("id").asText();
        db.upsert(id, node.get("document"));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
