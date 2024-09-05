package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class FileReaderTest {
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

  public void readDbData(BufferedReader reader) {
    try {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        JsonNode node = mapper.readTree(line);
        System.out.println(node.toPrettyString());
      }
    } catch (Exception e) {
      throw new RuntimeException();
    }
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
  public void testFileReaderDB() {
    FileReader file = new FileReader("rdbms");
    file.readHeader();
    List<Table> tables = file.readTables();
    Assertions.assertEquals(tables.get(0).name, "data");
    List<Index> indexes = file.readIndexes();
    Assertions.assertEquals(indexes.get(0).column, "name");
    List<User> users = file.readUsers();
    Assertions.assertEquals(users.get(0).username, "dbuser");
    List<Group> groups = file.readGroups();
    Assertions.assertEquals(groups.get(0).groupname, "sysdba");
    file.startDataStream();
    BufferedReader reader = file.getReader();
    readDbData(reader);
    file.close();
  }

  @Test
  public void testFileReaderCB() {
    FileReader file = new FileReader("couchbase");
    file.readHeader();
    List<Table> tables = file.readTables();
    Assertions.assertEquals(tables.get(0).name, "travel-sample");
    List<Index> indexes = file.readIndexes();
    Assertions.assertEquals(indexes.get(0).column, "name");
    List<User> users = file.readUsers();
    Assertions.assertEquals(users.get(0).username, "developer");
    List<Group> groups = file.readGroups();
    Assertions.assertEquals(groups.get(0).groupname, "devgroup");
    file.startDataStream();
    BufferedReader reader = file.getReader();
    readDbData(reader);
    file.close();
  }
}
