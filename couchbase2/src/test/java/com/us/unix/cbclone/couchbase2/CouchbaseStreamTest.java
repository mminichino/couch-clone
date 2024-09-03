package com.us.unix.cbclone.couchbase2;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;

public class CouchbaseStreamTest {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String BUCKET_PASSWORD = "couchbase.bucketPassword";
  public static final String LEGACY_AUTH = "couchbase.legacyAuth";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String TEST_DIRECTORY = "test.directory";
  public static final String DEFAULT_ADMIN_USER = "Administrator";
  public static final String DEFAULT_ADMIN_PASSWORD = "password";
  public static final String DEFAULT_BUCKET_PASSWORD = "";
  public static final String DEFAULT_LEGACY_AUTH = "false";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "test";

  @ParameterizedTest
  @ValueSource(strings = {"test.4.properties", "test.5.properties", "test.6.properties"})
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
    String bucketPass = properties.getProperty(BUCKET_PASSWORD, DEFAULT_BUCKET_PASSWORD);
    boolean legacyAuth = Boolean.parseBoolean(properties.getProperty(LEGACY_AUTH, DEFAULT_LEGACY_AUTH));
    String bucket = properties.getProperty(CLUSTER_BUCKET, DEFAULT_BUCKET);
    String directory = properties.getProperty(TEST_DIRECTORY, TEST_DIRECTORY);

    String[] parts = propertyFile.split("\\.");
    CouchbaseStream stream;
    if (!legacyAuth) {
      stream = new CouchbaseStream(hostname, username, password, bucket, true);
    } else {
      stream = new CouchbaseStream(hostname, bucketPass, bucket, true);
    }
    try {
      stream.toCompressedFile(String.format("%s/%s%s.gz", directory, bucket, parts[1]));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
