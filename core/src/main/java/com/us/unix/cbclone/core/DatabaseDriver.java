package com.us.unix.cbclone.core;

import java.io.BufferedReader;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

public abstract class DatabaseDriver {
  private final Properties properties = new Properties();
  public FileWriter writer;
  public FileReader reader;
  public String session = "dbdump";
  public boolean overwrite = false;
  public List<Table> tables;
  public String tableName;

  public Properties getProperties() {
    return properties;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void init(Properties properties) {
    this.properties.putAll(properties);
    this.initDb(this.properties);
    this.writer = new FileWriter(session, overwrite);
  }

  public void writeHeader() {
    this.writer.writeHeader();
  }

  public void writeTables() {
    tables = exportTables();
    this.writer.writeTables(tables);
  }

  public void writeIndexes() {
    this.writer.writeIndexes(exportIndexes());
  }

  public void writeUsers() {
    this.writer.writeUsers(exportUsers());
  }

  public void writeGroups() {
    this.writer.writeGroups(exportGroups());
  }

  public void writeData() {
    for (Table table : tables) {
      if (tableName != null && !tableName.equals(table.name)) {
        continue;
      }
      this.writer.startDataStream(table.name);
      Writer writer = this.writer.getWriter();
      exportData(writer, table);
    }
  }

  public void readHeader() {
    this.reader.readHeader();
  }

  public void readTables() {
    importTables(this.reader.readTables());
  }

  public void readIndexes() {
    importIndexes(this.reader.readIndexes());
  }

  public void readUsers() {
    importUsers(this.reader.readUsers());
  }

  public void readGroups() {
    importGroups(this.reader.readGroups());
  }

  public void readData() {
    for (String tableName = this.reader.startDataStream(); tableName != null; tableName = this.reader.startDataStream()) {
      Table table = Table.inList(tables, tableName);
      if (table == null) {
        throw new RuntimeException("Table not found: " + tableName);
      }
      importData(this.reader, table);
      this.reader.resetLine();
    }
  }

  public void shutdown() {
    this.writer.close();
  }

  public void exportDatabase() {
    writeHeader();
    writeTables();
    writeIndexes();
    writeUsers();
    writeGroups();
    writeData();
  }

  public void importDatabase() {
    readHeader();
    readTables();
    readIndexes();
    readUsers();
    readGroups();
    readData();
  }

  public abstract void initDb(Properties properties);

  public abstract void connectToTable(Table table);

  public abstract List<Table> exportTables();

  public abstract List<Index> exportIndexes();

  public abstract List<User> exportUsers();

  public abstract List<Group> exportGroups();

  public abstract void exportData(Writer writer, Table table);

  public abstract void importTables(List<Table> tables);

  public abstract void importIndexes(List<Index> indexes);

  public abstract void importUsers(List<User> users);

  public abstract void importGroups(List<Group> groups);

  public abstract void importData(FileReader reader, Table table);
}
