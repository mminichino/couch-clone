package com.us.unix.cbclone.core;

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class DBDriver {
  private final Properties properties = new Properties();
  public FileWriter file;
  public String session = "dbdump";
  public boolean overwrite = false;

  public Properties getProperties() {
    return properties;
  }

  public void init() {
    this.initDb();
    this.file = new FileWriter(session, overwrite);
  }

  public void writeHeader() {
    this.file.writeHeader();
  }

  public void writeTables() {
    this.file.writeTables(exportTables());
  }

  public void writeIndexes() {
    this.file.writeIndexes(exportIndexes());
  }

  public void writeUsers() {
    this.file.writeUsers(exportUsers());
  }

  public void writeGroups() {
    this.file.writeGroups(exportGroups());
  }

  public void writeData() {
    this.file.startDataStream();
    Writer writer = this.file.getWriter();
    exportData(writer);
  }

  public void shutdown() {
    this.file.close();
  }

  public void exportDatabase() {
    writeHeader();
    writeTables();
    writeIndexes();
    writeUsers();
    writeGroups();
    writeData();
  }

  public abstract void initDb();

  public abstract List<Table> exportTables();

  public abstract List<Index> exportIndexes();

  public abstract List<User> exportUsers();

  public abstract List<Group> exportGroups();

  public abstract void exportData(Writer writer);
}
