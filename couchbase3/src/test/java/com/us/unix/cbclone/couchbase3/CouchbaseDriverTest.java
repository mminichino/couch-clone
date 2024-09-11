package com.us.unix.cbclone.couchbase3;

import com.us.unix.cbclone.core.DatabaseDriver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;

public class CouchbaseDriverTest {

  @ParameterizedTest
  @ValueSource(strings = {"test.7.properties"})
  public void testCouchbaseDriverExport(String propertyFile) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties properties = new Properties();

    System.out.printf("Testing with properties file: %s%n", propertyFile);
    try {
      properties.load(loader.getResourceAsStream(propertyFile));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
    }

    DatabaseDriver driver = new CouchbaseDriver();

    driver.init(properties);
    driver.exportDatabase();
  }

  @ParameterizedTest
  @ValueSource(strings = {"test.import.properties"})
  public void testCouchbaseDriverImport(String propertyFile) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties properties = new Properties();

    System.out.printf("Testing with properties file: %s%n", propertyFile);
    try {
      properties.load(loader.getResourceAsStream(propertyFile));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
    }

    DatabaseDriver driver = new CouchbaseDriver();

//    driver.init(properties);
//    driver.importDatabase();
  }
}
