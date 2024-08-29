package com.us.unix.cbclone.couchbase2;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class CouchbaseStreamTest {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String TEST_DIRECTORY = "test.directory";
  public static final String DEFAULT_USER = "";
  public static final String DEFAULT_PASSWORD = "";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "test";

  @Test
  public void testBucketExport() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties properties = new Properties();

    try {
      properties.load(loader.getResourceAsStream("test.properties"));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
    }

    String hostname = properties.getProperty(CLUSTER_HOST, DEFAULT_HOSTNAME);
    String username = properties.getProperty(CLUSTER_USER, DEFAULT_USER);
    String password = properties.getProperty(CLUSTER_PASSWORD, DEFAULT_PASSWORD);
    String bucket = properties.getProperty(CLUSTER_BUCKET, DEFAULT_BUCKET);
    String directory = properties.getProperty(TEST_DIRECTORY, TEST_DIRECTORY);

    CouchbaseStream stream = new CouchbaseStream(hostname, username, password, bucket, true);
    try {
      stream.toCompressedFile(String.format("%s/%s.gz", directory, bucket));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
