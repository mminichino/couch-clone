package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FileReaderTest {
  private final ObjectMapper mapper = new ObjectMapper();

  public void readDbData(FileReader reader) {
    try {
      for (String line = reader.readLine(); line != null && !line.startsWith("__"); line = reader.readLine()) {
        JsonNode node = mapper.readTree(line);
        System.out.println(node.toPrettyString());
      }
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }

  @Test
  public void testFileReaderDB() {
    FileReader file = new FileReader("rdbms");
    file.readHeader();
    List<Table> tables = file.readTables();
    Assertions.assertEquals(tables.get(0).name, "customers");
    List<Index> indexes = file.readIndexes();
    Assertions.assertEquals(indexes.get(0).column, "name");
    List<User> users = file.readUsers();
    Assertions.assertEquals(users.get(0).username, "dbuser");
    List<Group> groups = file.readGroups();
    Assertions.assertEquals(groups.get(0).groupname, "sysdba");
    for (String table = file.startDataStream(); table != null; table = file.startDataStream()) {
      readDbData(file);
      file.resetLine();
    }
    file.close();
  }

  @Test
  public void testFileReaderCB() {
    FileReader file = new FileReader("couchbase");
    file.readHeader();
    List<Table> tables = file.readTables();
    Assertions.assertEquals(tables.get(0).name, "appdata.data.orders");
    List<Index> indexes = file.readIndexes();
    Assertions.assertEquals(indexes.get(0).column, "name");
    List<User> users = file.readUsers();
    Assertions.assertEquals(users.get(0).username, "developer");
    List<Group> groups = file.readGroups();
    Assertions.assertEquals(groups.get(0).groupname, "devgroup");
    for (String table = file.startDataStream(); table != null; table = file.startDataStream()) {
      readDbData(file);
      file.resetLine();
    }
    file.close();
  }
}
