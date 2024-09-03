package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;

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

  public User(JsonNode data) {
    this.username = data.has("id") ? data.get("id").asText() : null;
    this.password = data.has("password") ? data.get("password").asText() : null;
    this.name = data.has("name") ? data.get("name").asText() : null;
    this.email = null;
    this.groups = data.has("groups") ? getStringList(data.get("groups")) : new ArrayList<>();
    this.roles = data.has("roles") ? getObjectList(data.get("roles")) : new ArrayList<>();
  }

  private List<String> getStringList(JsonNode data) {
    Iterator<JsonNode> elements = data.elements();
    List<String> items = new ArrayList<>();
    while(elements.hasNext()){
      items.add(elements.next().asText());
    }
    return items;
  }

  private List<JsonNode> getObjectList(JsonNode data) {
    Iterator<JsonNode> elements = data.elements();
    List<JsonNode> items = new ArrayList<>();
    while(elements.hasNext()){
      items.add(elements.next());
    }
    return items;
  }
}
