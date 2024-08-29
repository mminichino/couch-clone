package com.us.unix.cbclone.couchbase2;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class CouchbaseConnectTest {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String ADMIN_USER = "couchbase.adminUsername";
  public static final String ADMIN_PASSWORD = "couchbase.adminPassword";
  public static final String BUCKET_PASSWORD = "couchbase.bucketPassword";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String TEST_DIRECTORY = "test.directory";
  public static final String DEFAULT_ADMIN_USER = "Administrator";
  public static final String DEFAULT_ADMIN_PASSWORD = "password";
  public static final String DEFAULT_BUCKET_PASSWORD = "";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "test";

  @Test
  public void testCouchbaseConnect() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties properties = new Properties();

    try {
      properties.load(loader.getResourceAsStream("test.properties"));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
    }

    String hostname = properties.getProperty(CLUSTER_HOST, DEFAULT_HOSTNAME);
    String adminUser = properties.getProperty(ADMIN_USER, DEFAULT_ADMIN_USER);
    String adminPass = properties.getProperty(ADMIN_PASSWORD, DEFAULT_ADMIN_PASSWORD);
    String bucketPass = properties.getProperty(BUCKET_PASSWORD, DEFAULT_BUCKET_PASSWORD);
    String bucket = properties.getProperty(CLUSTER_BUCKET, DEFAULT_BUCKET);
    String directory = properties.getProperty(TEST_DIRECTORY, TEST_DIRECTORY);

    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db = dbBuilder.connect(hostname, adminUser, adminPass).build();
    boolean result = db.isBucket(DEFAULT_BUCKET);
    Assertions.assertFalse(result);
    db.createBucket(DEFAULT_BUCKET);
    result = db.isBucket(DEFAULT_BUCKET);
    Assertions.assertTrue(result);
    db.dropBucket(DEFAULT_BUCKET);
    db.connectBucket(bucket);
    db.getIndexes();
  }
}
