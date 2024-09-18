package com.us.unix.cbclone.couchbase3;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.ReactiveCollection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.us.unix.cbclone.core.*;

import java.io.Writer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

public class CouchbaseDriver extends DatabaseDriver {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDriver.class);
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "default";
  public static final String BATCH_SIZE = "couchbase.batchSize";
  public static final String BATCH_SIZE_DEFAULT = "100";
  public static final String ENABLE_DEBUG = "couchbase.debug";
  public static final String ENABLE_DEBUG_DEFAULT = "false";
  public String hostname;
  public String username;
  public String password;
  public String bucket;
  public int batchSize = 100;
  public static volatile CouchbaseConnect db;
  public static volatile CouchbaseStream stream;
  private final PriorityBlockingQueue<Throwable> errorQueue = new PriorityBlockingQueue<>();
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void initDb(Properties properties) {
    hostname = properties.getProperty(CLUSTER_HOST, DEFAULT_HOSTNAME);
    username = properties.getProperty(CLUSTER_USER, DEFAULT_USER);
    password = properties.getProperty(CLUSTER_PASSWORD, DEFAULT_PASSWORD);
    batchSize = Integer.parseInt(properties.getProperty(BATCH_SIZE, BATCH_SIZE_DEFAULT));
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
    for (UserData user : users) {
      LOGGER.info("Importing user {}", user.getId());
      db.createUser(user.getId(), user.getPassword(), user.getName(), user.getGroups(), user.getRoles());
    }
  }

  @Override
  public void importGroups(List<GroupData> groups) {
    for (GroupData group : groups) {
      LOGGER.info("Importing group {}", group.getId());
      db.createGroup(group.getId(), group.getDescription(), group.getRoles());
    }
  }

  @Override
  public void cleanDb() {
    for (String bucket : db.listBuckets()) {
      LOGGER.info("Removing bucket {}", bucket);
      db.dropBucket(bucket);
    }
  }

  public void importBatch(ReactiveCollection collection, List<String> batch) {
    Flux.fromIterable(batch)
        .flatMap(record -> {
          try {
            JsonNode node = mapper.readTree(record);
            String id = node.get("metadata").get("id").asText();
            return collection.upsert(id, node.get("document"));
          } catch (JsonProcessingException e) {
            return Flux.error(new RuntimeException(e));
          }
        })
        .retryWhen(Retry.backoff(10, Duration.ofMillis(10)).filter(t -> t instanceof CouchbaseException))
        .doOnError(errorQueue::put)
        .blockLast();
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
    ReactiveCollection reactiveCollection = db.reactiveCollection();
    List<String> batch = new ArrayList<>(batchSize);
    try {
      for (String line = reader.readLine(); line != null && !line.equals("__END__"); line = reader.readLine()) {
        batch.add(line);
        if (batch.size() == batchSize) {
          importBatch(reactiveCollection, batch);
          batch.clear();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (!batch.isEmpty()) {
      importBatch(reactiveCollection, batch);
    }
    if (!errorQueue.isEmpty()) {
      LOGGER.warn("Some insert operations resulted in an error (see log for details)");
      for (Throwable t; (t = errorQueue.poll()) != null; ) {
        LOGGER.error(t.getMessage(), t);
      }
    }
  }
}
