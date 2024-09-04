package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Scope {
  public String name;
  public List<Collection> collections;
  private final ObjectMapper mapper = new ObjectMapper();

  public Scope(String name, List<Collection> collections) {
    this.name = name;
    this.collections = collections;
  }

  public Scope(String name) {
    this.name = name;
    this.collections = new ArrayList<>();
  }

  public Scope(JsonNode data) {
    this.name = data.get("name").asText();
    this.collections = getCollectionList(data.get("collections"));
  }

  public void addCollection(Collection collection) {
    this.collections.add(collection);
  }

  private List<Collection> getCollectionList(JsonNode data) {
    Iterator<JsonNode> elements = data.elements();
    List<Collection> items = new ArrayList<>();
    while(elements.hasNext()){
      items.add(new Collection(elements.next()));
    }
    return items;
  }

  private ArrayNode getCollectionArray(List<Collection> collections) {
    ArrayNode array = mapper.createArrayNode();
    for (Collection c : collections) {
      array.add(c.toJson());
    }
    return array;
  }

  public JsonNode toJson() {
    ObjectNode node = mapper.createObjectNode();
    node.put("name", name);
    node.set("collections", getCollectionArray(collections));
    return node;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }
}
