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
  public List<TableData> tables;
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
    for (TableData table : tables) {
      if (tableName != null && !tableName.equals(table.getName())) {
        continue;
      }
      this.writer.startDataStream(table.getName());
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
      TableData table = TableData.inList(tables, tableName);
      if (table == null) {
        throw new RuntimeException("Table not found: " + tableName);
      }
      importData(this.reader, table);
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

  public abstract void connectToTable(TableData table);

  public abstract List<TableData> exportTables();

  public abstract List<IndexData> exportIndexes();

  public abstract List<UserData> exportUsers();

  public abstract List<GroupData> exportGroups();

  public abstract void exportData(Writer writer, TableData table);

  public abstract void importTables(List<TableData> tables);

  public abstract void importIndexes(List<IndexData> indexes);

  public abstract void importUsers(List<UserData> users);

  public abstract void importGroups(List<GroupData> groups);

  public abstract void importData(FileReader reader, TableData table);
}
