package com.us.unix.cbclone.couchbase2;

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
  }

  @Override
  public void connectToTable(TableData table) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    db = dbBuilder
        .host(hostname)
        .username(username)
        .password(password)
        .bucketPassword(table.getPassword())
        .legacyAuth(legacyAuth)
        .build();
    db.connectBucket(table.getName());
  }

  @Override
  public List<TableData> exportTables() {
    return db.getBuckets();
  }

  @Override
  public List<IndexData> exportIndexes() {
    return db.getIndexes();
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
    if (!legacyAuth) {
      stream = db.stream(table.getName());
    } else {
      stream = db.stream(table.getName(), table.getPassword());
    }
    stream.toWriter(writer);
  }

  @Override
  public void importTables(List<TableData> tables) {

  }

  @Override
  public void importIndexes(List<IndexData> indexes) {

  }

  @Override
  public void importUsers(List<UserData> users) {

  }

  @Override
  public void importGroups(List<GroupData> groups) {

  }

  @Override
  public void importData(FileReader reader, TableData table) {

  }
}
