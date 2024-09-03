package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Group {
  public String groupname;
  public String description;
  public List<JsonNode> roles;

  public Group(String groupname, String description, List<JsonNode> roles) {
    this.groupname = groupname;
    this.description = description;
    this.roles = roles;
  }

  public Group(String groupname, String description) {
    this.groupname = groupname;
    this.description = description;
    this.roles = new ArrayList<>();
  }

  public Group(String groupname) {
    this.groupname = groupname;
    this.description = null;
    this.roles = new ArrayList<>();
  }

  public Group(JsonNode data) {
    this.groupname = data.has("id") ? data.get("id").asText() : null;
    this.description = data.has("description") ? data.get("description").asText() : null;
    this.roles = data.has("roles") ? getObjectList(data.get("roles")) : new ArrayList<>();
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
