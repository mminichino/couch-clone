package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

public class FileWriter {
  public FileOutputStream output;
  public Writer writer;
  public String filename;
  private final String dateStampFormat = "yyyy-MM-dd'T'HH:mm:ss";
  private final SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateStampFormat);
  private final Date date = new Date();
  private final ObjectMapper mapper = new ObjectMapper();

  public FileWriter(String filename) {
    this.filename = FileNameFormat.getFileAbsPath(filename, false);
    this.init();
  }

  public FileWriter(String filename, boolean overwrite) {
    this.filename = FileNameFormat.getFileAbsPath(filename, overwrite);
    this.init();
  }

  private void init() {
    try {
      output = new FileOutputStream(filename);
      writer = new OutputStreamWriter(new GZIPOutputStream(output), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getVersion() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties properties = new Properties();
    try {
      String VERSION_PROPERTIES = "version.properties";
      properties.load(loader.getResourceAsStream(VERSION_PROPERTIES));
      return properties.getProperty("version", "0.0.1");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeLine(String line) {
    try {
      writer.write(line + "\n");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Writer getWriter() {
    return writer;
  }

  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeHeader() {
    String label = "__HEADER__";
    String footer = "__END__";
    String timeStamp = timeStampFormat.format(date);

    try {
      writeLine(label);
      ObjectNode node = mapper.createObjectNode();
      node.put("version", getVersion());
      node.put("timestamp", timeStamp);
      writeLine(mapper.writeValueAsString(node));
      writeLine(footer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeTables(List<TableData> tables) {
    String label = "__TABLES__";
    String footer = "__END__";

    try {
      writeLine(label);
      for (TableData table : tables) {
        writeLine(mapper.writeValueAsString(table));
      }
      writeLine(footer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeIndexes(List<IndexData> indexes) {
    String label = "__INDEXES__";
    String footer = "__END__";

    try {
      writeLine(label);
      for (IndexData index : indexes) {
        writeLine(mapper.writeValueAsString(index));
      }
      writeLine(footer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeUsers(List<UserData> users) {
    String label = "__USERS__";
    String footer = "__END__";

    try {
      writeLine(label);
      for (UserData user : users) {
        writeLine(mapper.writeValueAsString(user));
      }
      writeLine(footer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeGroups(List<GroupData> groups) {
    String label = "__GROUPS__";
    String footer = "__END__";

    try {
      writeLine(label);
      for (GroupData group : groups) {
        writeLine(mapper.writeValueAsString(group));
      }
      writeLine(footer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void startDataStream(String table) {
    String label = "__DATA__:" + table;
    writeLine(label);
  }

  public void endDataStream() {
    String label = "__END__";
    writeLine(label);
  }
}
