package com.us.unix.cbclone.couchbase3;

import com.us.unix.cbclone.core.GroupData;
import com.us.unix.cbclone.core.TableData;
import com.us.unix.cbclone.core.UserData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;

public class CouchbaseConnectTest {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "test";

  @ParameterizedTest
  @ValueSource(strings = {"test.7.properties"})
  public void testCouchbaseConnect(String propertyFile) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties properties = new Properties();

    System.out.printf("Testing with properties file: %s%n", propertyFile);
    try {
      properties.load(loader.getResourceAsStream(propertyFile));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
    }

    String hostname = properties.getProperty(CLUSTER_HOST, DEFAULT_HOSTNAME);
    String username = properties.getProperty(CLUSTER_USER, DEFAULT_USER);
    String password = properties.getProperty(CLUSTER_PASSWORD, DEFAULT_PASSWORD);
    String bucket = properties.getProperty(CLUSTER_BUCKET, DEFAULT_BUCKET);

    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db = dbBuilder
        .host(hostname)
        .username(username)
        .password(password)
        .build();
    System.out.println(db.clusterVersion);
    boolean result = db.isBucket(DEFAULT_BUCKET);
    Assertions.assertFalse(result);
    db.createBucket(DEFAULT_BUCKET);
    result = db.isBucket(DEFAULT_BUCKET);
    Assertions.assertTrue(result);
    db.dropBucket(DEFAULT_BUCKET);
    db.connectBucket(bucket);
    for (TableData table : db.getBuckets()) {
      System.out.printf("%s.%s.%s\n", table.getName(), table.getScope().getName(), table.getCollection().getName());
      Assertions.assertNotNull(table.getName());
    }
    for (UserData user : db.getUsers()) {
      System.out.println(user.getId());
      Assertions.assertNotNull(user.getId());
    }
    for (GroupData group : db.getGroups()) {
      System.out.println(group.getId());
      Assertions.assertNotNull(group.getId());
    }
  }
}
