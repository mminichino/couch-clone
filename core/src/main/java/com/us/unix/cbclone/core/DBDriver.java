package com.us.unix.cbclone.core;

import java.io.BufferedReader;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

public abstract class DBDriver {
  private final Properties properties = new Properties();
  public FileWriter writer;
  public FileReader reader;
  public String session = "dbdump";
  public boolean overwrite = false;

  public Properties getProperties() {
    return properties;
  }

  public void init() {
    this.initDb();
    this.writer = new FileWriter(session, overwrite);
  }

  public void writeHeader() {
    this.writer.writeHeader();
  }

  public void writeTables() {
    this.writer.writeTables(exportTables());
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
    this.writer.startDataStream();
    Writer writer = this.writer.getWriter();
    exportData(writer);
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
    this.reader.startDataStream();
    BufferedReader reader = this.reader.getReader();
    importData(reader);
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

  public abstract void initDb();

  public abstract List<Table> exportTables();

  public abstract List<Index> exportIndexes();

  public abstract List<User> exportUsers();

  public abstract List<Group> exportGroups();

  public abstract void exportData(Writer writer);

  public abstract void importTables(List<Table> tables);

  public abstract void importIndexes(List<Index> indexes);

  public abstract void importUsers(List<User> users);

  public abstract void importGroups(List<Group> groups);

  public abstract void importData(BufferedReader reader);
}
