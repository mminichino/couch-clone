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
      for (String line = reader.readLine(); line != null && !line.equals("__END__"); line = reader.readLine()) {
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
    List<TableData> tables = file.readTables();
    Assertions.assertEquals(tables.get(0).getName(), "customers");
    List<UserData> users = file.readUsers();
    Assertions.assertEquals(users.get(0).getId(), "dbuser");
    List<GroupData> groups = file.readGroups();
    Assertions.assertEquals(groups.get(0).getId(), "sysdba");
    for (String table = file.startDataStream(); table != null; table = file.startDataStream()) {
      readDbData(file);
    }
    file.close();
  }

  @Test
  public void testFileReaderCB() {
    FileReader file = new FileReader("couchbase");
    file.readHeader();
    List<TableData> tables = file.readTables();
    Assertions.assertEquals(tables.get(0).getName(), "appdata.data.orders");
    List<UserData> users = file.readUsers();
    Assertions.assertEquals(users.get(0).getId(), "developer");
    List<GroupData> groups = file.readGroups();
    Assertions.assertEquals(groups.get(0).getId(), "devgroup");
    for (String table = file.startDataStream(); table != null; table = file.startDataStream()) {
      readDbData(file);
    }
    file.close();
  }
}
