package com.us.unix.cbclone.core;

import java.io.Writer;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DatabaseDriver {
  static final Logger LOGGER = LoggerFactory.getLogger(DatabaseDriver.class);
  private final Properties properties = new Properties();
  public FileWriter writer;
  public FileReader reader;
  public String session;
  public boolean exportMode;
  public boolean overwrite;
  public List<TableData> tables;
  public String tableName;

  public static final String SESSION_MODE = "cbclone.mode";
  public static final String SESSION_MODE_DEFAULT = "export";

  public static final String SESSION_PROPERTY = "cbclone.sessionName";
  public static final String SESSION_PROPERTY_DEFAULT = "dbclone";

  public static final String OVERWRITE_PROPERTY = "cbclone.overwrite";
  public static final String OVERWRITE_PROPERTY_DEFAULT = "true";

  public Properties getProperties() {
    return properties;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void init(Properties properties) {
    session = properties.getProperty(SESSION_PROPERTY, SESSION_PROPERTY_DEFAULT);
    overwrite = properties.getProperty(OVERWRITE_PROPERTY, OVERWRITE_PROPERTY_DEFAULT).equals("true");
    exportMode = properties.getProperty(SESSION_MODE, SESSION_MODE_DEFAULT).equals("export");

    LOGGER.info("Starting {} session with file {}", exportMode ? "export" : "import", session);
    this.properties.putAll(properties);
    this.initDb(this.properties);
    if (exportMode) {
      this.writer = new FileWriter(session, overwrite);
    } else {
      this.reader = new FileReader(session);
    }
  }

  public void writeHeader() {
    this.writer.writeHeader();
  }

  public void writeTables() {
    tables = exportTables();
    this.writer.writeTables(tables);
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
      String tableFullName;
      if (table.getScope().getName() != null && table.getCollection().getName() != null) {
        tableFullName = table.getName() + "." + table.getScope().getName() + "." + table.getCollection().getName();
      } else {
        tableFullName = table.getName();
      }
      this.writer.startDataStream(tableFullName);
      Writer writer = this.writer.getWriter();
      exportData(writer, table);
      this.writer.endDataStream();
    }
  }

  public void readHeader() {
    this.reader.readHeader();
  }

  public void readTables() {
    tables = this.reader.readTables();
    importTables(tables);
  }

  public void readUsers() {
    importUsers(this.reader.readUsers());
  }

  public void readGroups() {
    importGroups(this.reader.readGroups());
  }

  public void readData() {
    for (String tableName = this.reader.startDataStream(); tableName != null; tableName = this.reader.startDataStream()) {
      importData(this.reader, tableName);
    }
  }

  public void shutdown() {
    this.writer.close();
  }

  public void exportDatabase() {
    writeHeader();
    writeTables();
    writeUsers();
    writeGroups();
    writeData();
    writer.close();
  }

  public void importDatabase() {
    readHeader();
    readTables();
    readUsers();
    readGroups();
    readData();
    reader.close();
  }

  public abstract void initDb(Properties properties);

  public abstract List<TableData> exportTables();

  public abstract List<UserData> exportUsers();

  public abstract List<GroupData> exportGroups();

  public abstract void exportData(Writer writer, TableData table);

  public abstract void importTables(List<TableData> tables);

  public abstract void importUsers(List<UserData> users);

  public abstract void importGroups(List<GroupData> groups);

  public abstract void importData(FileReader reader, String table);

  public abstract void cleanDb();
}
