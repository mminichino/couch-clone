package com.us.unix.cbclone.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class FileWriterTest {
  private final ObjectMapper mapper = new ObjectMapper();

  public JsonNode getTestJson(String filename) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream in = loader.getResourceAsStream(filename)) {
      return mapper.readValue(in, JsonNode.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<TableData> generateDbTables() {
    List<TableData> tables = new ArrayList<>();
    List<ColumnData> columns = new ArrayList<>();

    ColumnData name = new ColumnData();
    name.setName("name");
    name.setType(DataType.STRING);
    ColumnData address = new ColumnData();
    address.setName("address");
    address.setType(DataType.STRING);
    columns.add(name);
    columns.add(address);
    IndexData nameIndex = new IndexData();
    nameIndex.setColumn("name");
    TableData table = new TableData();
    table.setName("customers");
    table.setSize(256);
    table.setColumns(columns);
    table.setIndexes(new ArrayList<>() {{ add(nameIndex); }});
    tables.add(table);

    columns = new ArrayList<>();
    ColumnData sku = new ColumnData();
    sku.setName("sku");
    sku.setType(DataType.STRING);
    ColumnData price = new ColumnData();
    price.setName("price");
    price.setType(DataType.FLOAT);
    columns.add(sku);
    columns.add(price);
    IndexData skuIndex = new IndexData();
    skuIndex.setColumn("sku");
    table = new TableData();
    table.setName("inventory");
    table.setSize(256);
    table.setColumns(columns);
    table.setIndexes(new ArrayList<>() {{ add(skuIndex); }});
    tables.add(table);

    return tables;
  }

  public List<TableData> generateCbTables() {
    List<TableData> tables = new ArrayList<>();
    JsonNode bucketJson = getTestJson("bucket.json");
    String name = bucketJson.get("name").asText();
    String type = bucketJson.get("bucketType").asText();
    int quota = bucketJson.get("quota").get("ram").asInt() / 1048576;
    int replicas = bucketJson.get("replicaNumber").asInt();
    String eviction = bucketJson.get("evictionPolicy").asText();
    int ttl = bucketJson.has("maxTTL") ? bucketJson.get("maxTTL").asInt() : 0;
    String storage = bucketJson.has("storageBackend") ? bucketJson.get("storageBackend").asText() : "couchstore";
    String resolution = bucketJson.has("conflictResolutionType") ? bucketJson.get("conflictResolutionType").asText() : "seqno";
    String password = bucketJson.has("saslPassword") ? bucketJson.get("saslPassword").asText() : "";

    BucketData b = new BucketData();
    b.setName(name);
    b.setType(type);
    b.setQuota(quota);
    b.setReplicas(replicas);
    b.setEviction(eviction);
    b.setTtl(ttl);
    b.setStorage(storage);
    b.setResolution(resolution);
    b.setPassword(password);

    IndexData idIndex = new IndexData();
    idIndex.setColumn("order_id");

    TableData t = new TableData();
    t.setName("appdata.data.orders");
    t.setSize(quota);
    t.setType(TableType.COUCHBASE);
    t.setBucket(b);
    ScopeData s = new ScopeData();
    s.setName("data");
    CollectionData c = new CollectionData();
    c.setName("orders");
    t.setScope(s);
    t.setCollection(c);
    t.setIndexes(new ArrayList<>() {{ add(idIndex); }});
    tables.add(t);

    IndexData nameIndex = new IndexData();
    nameIndex.setColumn("name");

    t = new TableData();
    t.setName("appdata.data.customers");
    t.setSize(quota);
    t.setType(TableType.COUCHBASE);
    t.setBucket(b);
    s = new ScopeData();
    s.setName("data");
    c = new CollectionData();
    c.setName("customers");
    t.setScope(s);
    t.setCollection(c);
    t.setIndexes(new ArrayList<>() {{ add(nameIndex); }});
    tables.add(t);

    return tables;
  }

  public List<IndexData> generateDbIndexes() {
    IndexData index = new IndexData();
    index.setColumn("name");
    index.setTable("customers");
    return new ArrayList<>() {{
      add(index);
    }};
  }

  public List<IndexData> generateCbIndexes() {
    IndexData index = new IndexData();
    index.setColumn("name");
    index.setTable("appdata.data.customers");
    index.setName("customers_idx");
    return new ArrayList<>() {{
      add(index);
    }};
  }

  public List<UserData> generateDbUsers() {
    UserData user = new UserData();
    user.setId("dbuser");
    return new ArrayList<>() {{
      add(user);
    }};
  }

  public List<UserData> generateCbUsers() {
    List<String> groups = new ArrayList<>();
    groups.add("devgroup");
    List<RoleData> roles = new ArrayList<>();
    RoleData role = new RoleData();
    role.setRole("admin");
    roles.add(role);
    UserData user = new UserData();
    user.setId("developer");
    user.setName("Dev User");
    user.setGroups(groups);
    user.setRoles(roles);
    return new ArrayList<>() {{
      add(user);
    }};
  }

  public List<GroupData> generateDbGroups() {
    GroupData group = new GroupData();
    group.setId("sysdba");
    return new ArrayList<>() {{
      add(group);
    }};
  }

  public List<GroupData> generateCbGroups() {
    List<RoleData> roles = new ArrayList<>();
    RoleData role = new RoleData();
    role.setRole("admin");
    roles.add(role);
    GroupData group = new GroupData();
    group.setId("devgroup");
    group.setDescription("Dev Group");
    group.setRoles(roles);
    return new ArrayList<>() {{
      add(group);
    }};
  }

  public void toWriter(Writer writer, String[] data) {
    Stream<String> stream = Arrays.stream(data);
    stream.forEach(record -> {
      try {
        writer.write(record + "\n");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public void generateDbData(Writer writer) {
    ObjectNode node = mapper.createObjectNode();
    node.put("name", "John Smith");
    node.put("age", 25);
    node.put("address", "123 Street Ln");
    List<String> rows = new ArrayList<>();
    rows.add(node.toString());
    String[] data = rows.toArray(new String[0]);
    toWriter(writer, data);
  }

  public void generateCbData(Writer writer) {
    ObjectNode node = mapper.createObjectNode();
    node.put("first_name", "John");
    node.put("list_name", "Smith");
    node.put("age", 25);
    node.put("address", "123 Street Ln");
    List<String> rows = new ArrayList<>();
    rows.add(node.toString());
    String[] data = rows.toArray(new String[0]);
    toWriter(writer, data);
  }

  @Test
  public void testFileWriterDB() {
    FileWriter file = new FileWriter("rdbms", true);
    file.writeHeader();
    List<TableData> tables = generateDbTables();
    file.startTableStream();
    for (TableData table : tables) {
      file.writeTable(table);
    }
    file.endTableStream();
    file.writeUsers(generateDbUsers());
    file.writeGroups(generateDbGroups());
    for (TableData table : tables) {
      file.startDataStream(table.getName());
      Writer writer = file.getWriter();
      generateDbData(writer);
      file.endDataStream();
    }
    file.close();
  }

  @Test
  public void testFileWriterCB() {
    FileWriter file = new FileWriter("couchbase", true);
    file.writeHeader();
    List<TableData> tables = generateCbTables();
    file.startTableStream();
    for (TableData table : tables) {
      file.writeTable(table);
    }
    file.endTableStream();
    file.writeUsers(generateCbUsers());
    file.writeGroups(generateCbGroups());
    for (TableData table : tables) {
      file.startDataStream(table.getName());
      Writer writer = file.getWriter();
      generateCbData(writer);
      file.endDataStream();
    }
    file.close();
  }
}
