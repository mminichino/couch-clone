package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

public class FileReader {
  public FileInputStream input;
  public BufferedReader reader;
  public String filename;
  public String version;
  public String timeStamp;
  private final ObjectMapper mapper = new ObjectMapper();

  public FileReader(String filename) {
    this.filename = FileNameFormat.readFileAbsPath(filename);
    this.init();
  }

  private void init() {
    try {
      input = new FileInputStream(filename);
      reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(input), StandardCharsets.UTF_8));
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

  public String readLine() {
    try {
      return reader.readLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public BufferedReader getReader() {
    return reader;
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void readHeader() {
    try {
      String header = readLine();
      if (!header.equals("__HEADER__")) {
        throw new RuntimeException("File format error, header marker not found");
      }
      String line = reader.readLine();
      JsonNode node = mapper.readTree(line);
      version = node.get("version").asText();
      timeStamp = node.get("timestamp").asText();
      String footer = readLine();
      if (!footer.equals("__END__")) {
        throw new RuntimeException("File format error, header footer not found");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<TableData> readTables() {
    List<TableData> tables = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__TABLES__")) {
      throw new RuntimeException("File format error, table marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.equals("__END__"); line = readLine()) {
        tables.add(mapper.readValue(line, TableData.class));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tables;
  }

  public List<IndexData> readIndexes() {
    List<IndexData> indexes = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__INDEXES__")) {
      throw new RuntimeException("File format error, index marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.equals("__END__"); line = readLine()) {
        indexes.add(mapper.readValue(line, IndexData.class));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return indexes;
  }

  public List<UserData> readUsers() {
    List<UserData> users = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__USERS__")) {
      throw new RuntimeException("File format error, user marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.equals("__END__"); line = readLine()) {
        users.add(mapper.readValue(line, UserData.class));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return users;
  }

  public List<GroupData> readGroups() {
    List<GroupData> groups = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__GROUPS__")) {
      throw new RuntimeException("File format error, group marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.equals("__END__"); line = readLine()) {
        groups.add(mapper.readValue(line, GroupData.class));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return groups;
  }

  public String startDataStream() {
    String header = readLine();
    if (header == null) {
      return null;
    }
    if (!header.startsWith("__DATA__")) {
      throw new RuntimeException("File format error, data marker not found");
    }
    String[] parts = header.split(":");
    return parts[1];
  }
}
