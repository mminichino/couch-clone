package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Scope {
  public String name;
  public List<Collection> collections;

  public Scope(String name, List<Collection> collections) {
    this.name = name;
    this.collections = collections;
  }

  public Scope(String name) {
    this.name = name;
    this.collections = new ArrayList<>();
  }

  public Scope(JsonNode data) {
    this.name = data.has("name") ? data.get("name").asText() : "_default";
    this.collections = data.has("collections") ? getObjectList(data.get("collections")) : new ArrayList<>();
  }

  private List<Collection> getObjectList(JsonNode data) {
    Iterator<JsonNode> elements = data.elements();
    List<Collection> items = new ArrayList<>();
    while(elements.hasNext()){
      items.add(new Collection(elements.next()));
    }
    return items;
  }
}
