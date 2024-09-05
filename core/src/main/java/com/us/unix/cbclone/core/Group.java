package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Group {
  public String groupname;
  public String description;
  public List<JsonNode> roles;
  private final ObjectMapper mapper = new ObjectMapper();

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
    this.groupname = data.get("id").asText();
    this.description = data.get("description").asText();
    this.roles = getRoleList(data.get("roles"));
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
    node.put("id", groupname);
    node.put("description", description);
    node.set("roles", mapper.valueToTree(roles));
    return node;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }
}
