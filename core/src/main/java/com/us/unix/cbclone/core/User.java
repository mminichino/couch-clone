package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class User {
  public String username;
  public String password;
  public String name;
  public String email;
  public List<String> groups;
  public List<JsonNode> roles;
  private final ObjectMapper mapper = new ObjectMapper();

  public User(String username, String password, String name, String email, List<String> groups, List<JsonNode> roles) {
    this.username = username;
    this.password = password;
    this.name = name;
    this.email = email;
    this.groups = groups;
    this.roles = roles;
  }

  public User(String username, String password, String name, String email, List<String> groups) {
    this.username = username;
    this.password = password;
    this.name = name;
    this.email = email;
    this.groups = groups;
    this.roles = new ArrayList<>();
  }

  public User(String username, String password, String name, String email) {
    this.username = username;
    this.password = password;
    this.name = name;
    this.email = email;
    this.groups = new ArrayList<>();
    this.roles = new ArrayList<>();
  }

  public User(String username, String password, String name) {
    this.username = username;
    this.password = password;
    this.name = name;
    this.email = null;
    this.groups = new ArrayList<>();
    this.roles = new ArrayList<>();
  }

  public User(String username, String password) {
    this.username = username;
    this.password = password;
    this.name = null;
    this.email = null;
    this.groups = new ArrayList<>();
    this.roles = new ArrayList<>();
  }

  public User(String username) {
    this.username = username;
    this.password = null;
    this.name = null;
    this.email = null;
    this.groups = new ArrayList<>();
    this.roles = new ArrayList<>();
  }

  public User(JsonNode data) {
    this.username = data.get("id").asText();
    this.password = data.get("password").asText();
    this.name = data.get("name").asText();
    this.email = data.get("email").asText();
    this.groups = getGroupList(data.get("groups"));
    this.roles = getRoleList(data.get("roles"));
  }

  private List<String> getGroupList(JsonNode data) {
    Iterator<JsonNode> elements = data.elements();
    List<String> items = new ArrayList<>();
    while(elements.hasNext()){
      items.add(elements.next().asText());
    }
    return items;
  }

  private List<JsonNode> getRoleList(JsonNode data) {
    Iterator<JsonNode> elements = data.elements();
    List<JsonNode> items = new ArrayList<>();
    while(elements.hasNext()){
      items.add(elements.next());
    }
    return items;
  }

  public JsonNode toJson() {
    ObjectNode node = mapper.createObjectNode();
    node.put("id", username);
    node.put("password", password);
    node.put("name", name);
    node.put("email", email);
    node.set("groups", mapper.valueToTree(groups));
    node.set("roles", mapper.valueToTree(roles));
    return node;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }
}
