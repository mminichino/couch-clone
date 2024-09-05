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

  public List<Index> generateDbIndexes() {
    Index index = new Index("name", "data");
    return new ArrayList<>() {{
      add(index);
    }};
  }

  public List<Index> generateCbIndexes() {
    Index index = new Index("name", "data", "data_name_ix");
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
    file.writeTables(generateDbTables());
    file.writeIndexes(generateDbIndexes());
    file.writeUsers(generateDbUsers());
    file.writeGroups(generateDbGroups());
    file.startDataStream();
    Writer writer = file.getWriter();
    generateDbData(writer);
    file.close();
  }

  @Test
  public void testFileWriterCB() {
    FileWriter file = new FileWriter("couchbase", true);
    file.writeHeader();
    file.writeTables(generateCbTables());
    file.writeIndexes(generateCbIndexes());
    file.writeUsers(generateCbUsers());
    file.writeGroups(generateCbGroups());
    file.startDataStream();
    Writer writer = file.getWriter();
    generateCbData(writer);
    file.close();
  }
}
