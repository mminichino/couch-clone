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
    byte[] bytes = label.getBytes(StandardCharsets.UTF_8);
    String timeStamp = timeStampFormat.format(date);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(bytes);
      writeLine(outputStream.toString(StandardCharsets.UTF_8));
      ObjectNode node = mapper.createObjectNode();
      node.put("version", getVersion());
      node.put("timestamp", timeStamp);
      writeLine(mapper.writeValueAsString(node));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeTables(List<Table> tables) {
    String label = "__TABLES__";
    byte[] bytes = label.getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(bytes);
      writeLine(outputStream.toString(StandardCharsets.UTF_8));
      for (Table table : tables) {
        writeLine(table.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeIndexes(List<Index> indexes) {
    String label = "__INDEXES__";
    byte[] bytes = label.getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(bytes);
      writeLine(outputStream.toString(StandardCharsets.UTF_8));
      for (Index index : indexes) {
        writeLine(index.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeUsers(List<User> users) {
    String label = "__USERS__";
    byte[] bytes = label.getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(bytes);
      writeLine(outputStream.toString(StandardCharsets.UTF_8));
      for (User user : users) {
        writeLine(user.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeGroups(List<Group> groups) {
    String label = "__GROUPS__";
    byte[] bytes = label.getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(bytes);
      writeLine(outputStream.toString(StandardCharsets.UTF_8));
      for (Group group : groups) {
        writeLine(group.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void startDataStream(String table) {
    String label = "__DATA__:" + table;
    byte[] bytes = label.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(bytes);
      writeLine(outputStream.toString(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
