package com.us.unix.cbclone.couchbase3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.us.unix.cbclone.core.*;

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseDriver extends DatabaseDriver {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDriver.class);
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
  public static final String ENABLE_DEBUG = "couchbase.debug";
  public static final String ENABLE_DEBUG_DEFAULT = "false";
  public String hostname;
  public String username;
  public String password;
  public String bucket;
  public static volatile CouchbaseConnect db;
  public static volatile CouchbaseStream stream;

  private final List<Future<Status>> tasks = new ArrayList<>();
  private final ExecutorService executor = Executors.newFixedThreadPool(32);
  private final PriorityBlockingQueue<String> queue = new PriorityBlockingQueue<>();

  @Override
  public void initDb(Properties properties) {
    hostname = properties.getProperty(CLUSTER_HOST, DEFAULT_HOSTNAME);
    username = properties.getProperty(CLUSTER_USER, DEFAULT_USER);
    password = properties.getProperty(CLUSTER_PASSWORD, DEFAULT_PASSWORD);
    boolean debug = getProperties().getProperty(ENABLE_DEBUG, ENABLE_DEBUG_DEFAULT).equals("true");

    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    db = dbBuilder
        .host(hostname)
        .username(username)
        .password(password)
        .enableDebug(debug)
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
  public void cleanDb() {
    for (String bucket : db.listBuckets()) {
      LOGGER.info("Removing bucket {}", bucket);
      db.dropBucket(bucket);
    }
  }

  public void taskAdd(Callable<Status> task) {
    tasks.add(executor.submit(task));
  }

  public boolean taskWait() {
    boolean status = true;
    for (Future<Status> future : tasks) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error(e.getMessage(), e);
        status = false;
      }
    }
    tasks.clear();
    return status;
  }

  public Status upsertDocument(String record) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readTree(record);
      String id = node.get("metadata").get("id").asText();
      db.upsert(id, node.get("document"));
      return Status.OK;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void importData(FileReader reader, String table) {
    String[] keyspace = table.split("\\.");
    String bucketName = keyspace[0];
    String scopeName = keyspace[1];
    String collectionName = keyspace[2];
    LOGGER.info("Importing keyspace {}", table);
    db.connectBucket(bucketName);
    db.connectScope(scopeName);
    db.connectCollection(collectionName);
    try {
      for (String line = reader.readLine(); line != null && !line.equals("__END__"); line = reader.readLine()) {
        String finalLine = line;
        taskAdd(() -> upsertDocument(finalLine));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (!taskWait()) {
      throw new RuntimeException("Import failed (see log for details)");
    }
  }
}
