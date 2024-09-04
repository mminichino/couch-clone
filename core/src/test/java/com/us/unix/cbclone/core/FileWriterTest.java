package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class FileWriterTest {
  private final ObjectMapper mapper = new ObjectMapper();

  public JsonNode getTestJson(String filename) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream in = loader.getResourceAsStream(filename)) {
      return mapper.readValue(in, JsonNode.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Table> generateDbTables() {
    List<Column> columns = new ArrayList<>();
    columns.add(new Column("name", DataType.STRING));
    Table table = new Table("data", 256, columns);
    return new ArrayList<>() {{
      add(table);
    }};
  }

  public List<Table> generateCbTables() {
    List<Scope> scopes = new ArrayList<>();
    Scope scope = new Scope("data");
    scope.addCollection(new Collection("orders", 0));
    scope.addCollection(new Collection("customers", 0));
    scopes.add(scope);
    Table table = new Table(getTestJson("bucket.json"), scopes);
    return new ArrayList<>() {{
      add(table);
    }};
  }

  @Test
  public void testFileWriterDB() {
    FileWriter file = new FileWriter("rdbms", true);
    file.writeHeader();
    file.writeTables(generateDbTables());
    file.close();
  }

  @Test
  public void testFileWriterCB() {
    FileWriter file = new FileWriter("couchbase", true);
    file.writeHeader();
    file.writeTables(generateCbTables());
    file.close();
  }
}
