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
      reader.mark(1);
      return reader.readLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void resetLine() {
    try {
      reader.reset();
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
    String header = readLine();
    if (!header.equals("__HEADER__")) {
      throw new RuntimeException("File format error, header marker not found");
    }
    try {
      String line = reader.readLine();
      JsonNode node = mapper.readTree(line);
      version = node.get("version").asText();
      timeStamp = node.get("timestamp").asText();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Table> readTables() {
    List<Table> tables = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__TABLES__")) {
      throw new RuntimeException("File format error, table marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.startsWith("__"); line = readLine()) {
        JsonNode node = mapper.readTree(line);
        tables.add(new Table(node));
      }
      resetLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tables;
  }

  public List<Index> readIndexes() {
    List<Index> indexes = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__INDEXES__")) {
      throw new RuntimeException("File format error, index marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.startsWith("__"); line = readLine()) {
        JsonNode node = mapper.readTree(line);
        indexes.add(new Index(node));
      }
      resetLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return indexes;
  }

  public List<User> readUsers() {
    List<User> users = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__USERS__")) {
      throw new RuntimeException("File format error, user marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.startsWith("__"); line = readLine()) {
        JsonNode node = mapper.readTree(line);
        users.add(new User(node));
      }
      resetLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return users;
  }

  public List<Group> readGroups() {
    List<Group> groups = new ArrayList<>();
    String header = readLine();
    if (!header.equals("__GROUPS__")) {
      throw new RuntimeException("File format error, group marker not found");
    }
    try {
      for (String line = readLine(); line != null && !line.startsWith("__"); line = readLine()) {
        JsonNode node = mapper.readTree(line);
        groups.add(new Group(node));
      }
      resetLine();
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
