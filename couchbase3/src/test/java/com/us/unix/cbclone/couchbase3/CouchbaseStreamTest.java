package com.us.unix.cbclone.couchbase3;

import com.us.unix.cbclone.core.TableData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;

public class CouchbaseStreamTest {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String TEST_DIRECTORY = "test.directory";
  public static final String DEFAULT_ADMIN_USER = "Administrator";
  public static final String DEFAULT_ADMIN_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "test";

  @ParameterizedTest
  @ValueSource(strings = {"test.7.properties"})
  public void testBucketExport(String propertyFile) {
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
    String username = properties.getProperty(CLUSTER_USER, DEFAULT_ADMIN_USER);
    String password = properties.getProperty(CLUSTER_PASSWORD, DEFAULT_ADMIN_PASSWORD);
    String bucket = properties.getProperty(CLUSTER_BUCKET, DEFAULT_BUCKET);
    String directory = properties.getProperty(TEST_DIRECTORY, TEST_DIRECTORY);

    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db = dbBuilder
        .host(hostname)
        .username(username)
        .password(password)
        .build();

    String[] parts = propertyFile.split("\\.");
    try {
      for (TableData table : db.getBuckets()) {
        String bucketName = table.getName();
        String scopeName = table.getScope().getName();
        String collectionName = table.getCollection().getName();
        System.out.printf("Streaming => %s.%s.%s\n", bucketName, scopeName, collectionName);
        CouchbaseStream stream = db.stream(bucketName, scopeName, collectionName);
        stream.toCompressedFile(String.format("%s/%s-%s.%s.%s.gz", directory, parts[1], bucketName, scopeName, collectionName));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
