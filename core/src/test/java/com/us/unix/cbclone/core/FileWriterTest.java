package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

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
    List<Table> tables = new ArrayList<>();
    List<Column> columns = new ArrayList<>();
    columns.add(new Column("name", DataType.STRING));
    columns.add(new Column("address", DataType.STRING));
    Table table = new Table("customers", 256, columns);
    tables.add(table);
    columns = new ArrayList<>();
    columns.add(new Column("sku", DataType.STRING));
    columns.add(new Column("price", DataType.FLOAT));
    table = new Table("inventory", 256, columns);
    tables.add(table);
    return tables;
  }

  public List<Table> generateCbTables() {
    List<Table> tables = new ArrayList<>();
    Table table = new Table(getTestJson("bucket.json"), "data", "orders");
    tables.add(table);
    table = new Table(getTestJson("bucket.json"), "data", "customers");
    tables.add(table);
    return tables;
  }

  public List<Index> generateDbIndexes() {
    Index index = new Index("name", "customers");
    return new ArrayList<>() {{
      add(index);
    }};
  }

  public List<Index> generateCbIndexes() {
    Index index = new Index("name", "appdata.data.customers", "customers_idx");
    return new ArrayList<>() {{
      add(index);
    }};
  }

  public List<User> generateDbUsers() {
    User user = new User("dbuser");
    return new ArrayList<>() {{
      add(user);
    }};
  }

  public List<User> generateCbUsers() {
    List<String> groups = new ArrayList<>();
    groups.add("devgroup");
    List<JsonNode> roles = new ArrayList<>();
    roles.add(getTestJson("role.json"));

    User user = new User("developer", null, "Dev User", null, groups, roles);
    return new ArrayList<>() {{
      add(user);
    }};
  }

  public List<Group> generateDbGroups() {
    Group group = new Group("sysdba");
    return new ArrayList<>() {{
      add(group);
    }};
  }

  public List<Group> generateCbGroups() {
    List<JsonNode> roles = new ArrayList<>();
    ObjectNode role = mapper.createObjectNode();
    role.put("role", "admin");
    roles.add(role);
    Group group = new Group("devgroup", "Dev Group", roles);
    return new ArrayList<>() {{
      add(group);
    }};
  }

  public void toWriter(Writer writer, String[] data) {
    Stream<String> stream = Arrays.stream(data);
    stream.forEach(record -> {
      try {
        writer.write(record + "\n");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public void generateDbData(Writer writer) {
    ObjectNode node = mapper.createObjectNode();
    node.put("name", "John Smith");
    node.put("age", 25);
    node.put("address", "123 Street Ln");
    List<String> rows = new ArrayList<>();
    rows.add(node.toString());
    String[] data = rows.toArray(new String[0]);
    toWriter(writer, data);
  }

  public void generateCbData(Writer writer) {
    ObjectNode node = mapper.createObjectNode();
    node.put("first_name", "John");
    node.put("list_name", "Smith");
    node.put("age", 25);
    node.put("address", "123 Street Ln");
    List<String> rows = new ArrayList<>();
    rows.add(node.toString());
    String[] data = rows.toArray(new String[0]);
    toWriter(writer, data);
  }

  @Test
  public void testFileWriterDB() {
    FileWriter file = new FileWriter("rdbms", true);
    file.writeHeader();
    List<Table> tables = generateDbTables();
    file.writeTables(tables);
    file.writeIndexes(generateDbIndexes());
    file.writeUsers(generateDbUsers());
    file.writeGroups(generateDbGroups());
    for (Table table : tables) {
      file.startDataStream(table.name);
      Writer writer = file.getWriter();
      generateDbData(writer);
    }
    file.close();
  }

  @Test
  public void testFileWriterCB() {
    FileWriter file = new FileWriter("couchbase", true);
    file.writeHeader();
    List<Table> tables = generateCbTables();
    file.writeTables(tables);
    file.writeIndexes(generateCbIndexes());
    file.writeUsers(generateCbUsers());
    file.writeGroups(generateCbGroups());
    for (Table table : tables) {
      file.startDataStream(table.name);
      Writer writer = file.getWriter();
      generateCbData(writer);
    }
    file.close();
  }
}
