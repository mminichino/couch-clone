package com.us.unix.cbclone.core;

import java.util.HashSet;
import java.util.Set;

public class Column {
  public String name;
  public DataType type;
  public Set<String> keys;

  public Column(String name, DataType type, Set<String> keys) {
    this.name = name;
    this.type = type;
    this.keys = keys;
  }

  public Column(String name, DataType type) {
    this.name = name;
    this.type = type;
    this.keys = new HashSet<>();
  }
}
