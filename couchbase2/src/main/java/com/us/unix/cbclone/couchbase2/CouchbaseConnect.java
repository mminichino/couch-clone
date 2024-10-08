package com.us.unix.cbclone.couchbase2;

import com.couchbase.client.java.*;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.BucketAlreadyExistsException;
import com.couchbase.client.java.error.BucketDoesNotExistException;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.codelry.util.rest.REST;
import com.us.unix.cbclone.core.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Couchbase Connection Utility.
 */
public final class CouchbaseConnect {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseConnect.class);
  private volatile Cluster cluster;
  private volatile ClusterManager clusterManager;
  private volatile Bucket bucket;
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_BUCKET_PASSWORD = "";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final Boolean DEFAULT_SSL_MODE = false;
  public static final String DEFAULT_SSL_SETTING = "true";
  private static final String DEFAULT_PROJECT = null;
  private static final String DEFAULT_DATABASE = null;
  private static final Object STARTUP_COORDINATOR = new Object();
  private final String hostname;
  private final String username;
  private final String password;
  private String bucketPassword;
  private int bucketReplicas;
  private final boolean legacyAuth;
  private final String rootCert;
  private final String clientCert;
  private String project;
  private String database;
  private boolean external;
  private String bucketName;
  private final Boolean useSsl;
  public int adminPort;
  public int searchPort;
  private final int ttlSeconds;
  private final ObjectMapper mapper = new ObjectMapper();
  private ArrayNode hostMap = mapper.createArrayNode();
  private JsonNode clusterInfo = mapper.createObjectNode();
  public String clusterVersion;
  public int majorRevision;
  public int minorRevision;
  public int patchRevision;
  private final boolean enableDebug;

  /**
   * Builder Class.
   */
  public static class CouchbaseBuilder {
    private String hostname = DEFAULT_HOSTNAME;
    private String username = DEFAULT_USER;
    private String password = DEFAULT_PASSWORD;
    private String bucketPass = DEFAULT_BUCKET_PASSWORD;
    private String rootCert;
    private String clientCert;
    private Boolean sslMode = DEFAULT_SSL_MODE;
    private Boolean legacyAuth = false;
    private Boolean enableDebug = false;
    private String bucketName;
    private int bucketReplicas = 1;
    private int ttlSeconds = 0;

    public CouchbaseBuilder ttl(int value) {
      this.ttlSeconds = value;
      return this;
    }

    public CouchbaseBuilder host(final String name) {
      this.hostname = name;
      return this;
    }

    public CouchbaseBuilder username(final String name) {
      this.password = name;
      return this;
    }

    public CouchbaseBuilder password(final String name) {
      this.password = name;
      return this;
    }

    public CouchbaseBuilder bucketPassword(final String name) {
      this.bucketPass = name;
      return this;
    }

    public CouchbaseBuilder bucketReplicas(final int count) {
      this.bucketReplicas = count;
      return this;
    }

    public CouchbaseBuilder rootCert(final String name) {
      this.rootCert = name;
      return this;
    }

    public CouchbaseBuilder clientKeyStore(final String name) {
      this.clientCert = name;
      return this;
    }

    public CouchbaseBuilder legacyAuth(final Boolean mode) {
      this.legacyAuth = mode;
      return this;
    }

    public CouchbaseBuilder connect(final String host, final String user, final String password) {
      this.hostname = host;
      this.username = user;
      this.password = password;
      return this;
    }

    public CouchbaseBuilder ssl(final Boolean mode) {
      this.sslMode = mode;
      return this;
    }

    public CouchbaseBuilder bucket(final String name) {
      this.bucketName = name;
      return this;
    }

    public CouchbaseBuilder enableDebug(final Boolean mode) {
      this.enableDebug = mode;
      return this;
    }

    public CouchbaseConnect build() {
      return new CouchbaseConnect(this);
    }
  }

  private CouchbaseConnect(CouchbaseBuilder builder) {
    hostname = builder.hostname;
    username = builder.username;
    password = builder.password;
    bucketPassword = builder.bucketPass;
    rootCert = builder.rootCert;
    clientCert = builder.clientCert;
    legacyAuth = builder.legacyAuth;
    enableDebug = builder.enableDebug;
    useSsl = builder.sslMode;
    ttlSeconds = builder.ttlSeconds;
    bucketName = builder.bucketName;
    bucketReplicas = builder.bucketReplicas;
    connect();
  }

  public void connect() {
    String couchbasePrefix;

    if (useSsl) {
      couchbasePrefix = "couchbases://";
      adminPort = 18091;
      searchPort = 18094;
    } else {
      couchbasePrefix = "couchbase://";
      adminPort = 8091;
      searchPort = 8094;
    }

    String connectString = couchbasePrefix + hostname;

    synchronized (STARTUP_COORDINATOR) {
      try {
        if (cluster == null) {

          cluster = CouchbaseCluster.create(hostname);
          if (!legacyAuth) {
            cluster.authenticate(username, password);
          }
          clusterManager = cluster.clusterManager(username, password);
          getClusterInfo();
        }
      } catch(Exception e) {
        logError(e, connectString);
      }
    }
  }

  public void disconnect() {
    bucket = null;
    if (cluster != null) {
      cluster.disconnect();
    }
    cluster = null;
    hostMap = mapper.createArrayNode();
    clusterInfo = mapper.createObjectNode();
  }

  public CouchbaseStream stream(String bucketName) {
    return new CouchbaseStream(hostname, username, password, bucketName, true);
  }

  public CouchbaseStream stream(String bucketName, String bucketPassword) {
    return new CouchbaseStream(hostname, bucketPassword, bucketName, true);
  }

  public String hostValue() {
    return hostname;
  }

  public String adminUserValue() {
    return username;
  }

  public String adminPasswordValue() {
    return password;
  }

  public boolean externalValue() {
    return external;
  }

  public String getBucketName() {
    return bucketName;
  }

  public Cluster getCluster() {
    return cluster;
  }

  private void logError(Exception error, String connectString) {
    LOGGER.error(String.format("Connection string: %s", connectString));
    LOGGER.error(error.getMessage(), error);
  }

  private void getClusterInfo() {
    ClusterInfo clusterData = clusterManager.info();
    clusterVersion = clusterData.getMinVersion().toString();
    majorRevision = Integer.parseInt(clusterVersion.split("\\.")[0]);
    minorRevision = Integer.parseInt(clusterVersion.split("\\.")[1]);
    patchRevision = Integer.parseInt(clusterVersion.split("\\.")[2]);
    try {
      clusterInfo = mapper.readTree(clusterData.raw().toString());
      for (JsonNode node : clusterInfo.get("nodes")) {
        String hostEntry = node.get("hostname").asText();
        String[] endpoint = hostEntry.split(":", 2);
        String hostname = endpoint[0];
        JsonNode services = node.get("services");

        ObjectNode entry = mapper.createObjectNode();
        entry.put("hostname", hostname);
        entry.set("services", services);

        hostMap.add(entry);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean searchEnabled() {
    Stream<JsonNode> stream = StreamSupport.stream(hostMap.spliterator(), false);
    return stream
        .map(e -> StreamSupport.stream(e.get("services").spliterator(), false)
            .map(JsonNode::asText).collect(Collectors.toList()))
        .anyMatch(l -> l.contains("fts"));
  }

  public List<String> searchNodes() {
    Stream<JsonNode> stream = StreamSupport.stream(hostMap.spliterator(), false);
    return stream
        .filter(e -> StreamSupport.stream(e.get("services").spliterator(), false)
            .map(JsonNode::asText).anyMatch(s -> s.equals("fts")))
        .map(e -> e.get("hostname").asText())
        .collect(Collectors.toList());
  }

  private int getMemQuota() {
    int used = (int) (clusterInfo.get("storageTotals").get("ram").get("quotaUsedPerNode").asLong() / 1048576);
    int total = (int) (clusterInfo.get("storageTotals").get("ram").get("quotaTotalPerNode").asLong() / 1048576);
    return total - used;
  }

  public List<String> listBuckets() {
    REST client = new REST(hostname, username, password, useSsl, adminPort).enableDebug(enableDebug);
    try {
      String endpoint = "pools/default/buckets";
      return client.get(endpoint).validate().json().findValuesAsText("name");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Boolean isBucket(String bucket) {
    List<String> results = listBuckets();
    return results.contains(bucket);
  }

  public void connectBucket() {
    if (!bucketPassword.isEmpty()) {
      bucket = cluster.openBucket(bucketName, bucketPassword);
    } else {
      bucket = cluster.openBucket(bucketName);
    }
  }

  public void connectBucket(String name) {
    this.bucketName = name;
    if (!bucketPassword.isEmpty()) {
      bucket = cluster.openBucket(bucketName, bucketPassword);
    } else {
      bucket = cluster.openBucket(bucketName);
    }
  }

  public void connectBucket(String name, String password) {
    this.bucketName = name;
    this.bucketPassword = password;
    if (!bucketPassword.isEmpty()) {
      bucket = cluster.openBucket(bucketName, bucketPassword);
    } else {
      bucket = cluster.openBucket(bucketName);
    }
  }

  public void createBucket(String name) {
    int quota = getMemQuota();
    bucketCreate(name, quota);
  }

  public void createBucket(String name, int quota) {
    bucketCreate(name, quota);
  }

  public void createBucket(String name, int quota, int replicas) {
    this.bucketReplicas = replicas;
    bucketCreate(name, quota);
  }

  public void createBucket(String name, int quota, int replicas, String password) {
    this.bucketReplicas = replicas;
    this.bucketPassword = password;
    bucketCreate(name, quota);
  }

  public void bucketCreate(String name, int quota) {
    BucketSettings bucketSettings = new DefaultBucketSettings.Builder()
        .type(BucketType.COUCHBASE)
        .name(name)
        .password(bucketPassword)
        .quota(quota)
        .replicas(bucketReplicas)
        .indexReplicas(true)
        .enableFlush(true)
        .build();
    try {
      clusterManager.insertBucket(bucketSettings);
    } catch (BucketAlreadyExistsException e) {
      LOGGER.info("Create: Bucket {} already exists", name);
    }
  }

  public void dropBucket(String name) {
    try {
      clusterManager.removeBucket(name);
    } catch (BucketDoesNotExistException e) {
      LOGGER.info("Drop: Bucket {} does not exist", name);
    }
  }

  public JsonObject get(String id) {
    if (bucket == null) {
      throw new RuntimeException("Bucket is not connected");
    }
    try {
      return bucket.get(id).content();
    } catch (DocumentDoesNotExistException e) {
      return null;
    }
  }

  public void upsert(String id, JsonObject content) {
    if (bucket == null) {
      throw new RuntimeException("Bucket is not connected");
    }
    try {
      bucket.upsert(JsonDocument.create(id, content));
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public List<JsonObject> query(String queryString) {
    if (bucket == null) {
      throw new RuntimeException("Bucket is not connected");
    }
    final List<JsonObject> data = Collections.synchronizedList(new ArrayList<>());
    try {
      for (N1qlQueryRow item : bucket.query(N1qlQuery.simple(queryString, N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)))) {
        for (String name : item.value().getNames()) {
          data.add(item.value().getObject(name));
        }
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
    return data;
  }

  public List<String> getStringList(JsonArray node) {
    List<String> result = new ArrayList<>();
    node.forEach(item -> result.add(item.toString()));
    return result;
  }

  public List<SearchIndexData> getSearchIndexes(String bucket) {
    List<SearchIndexData> result = new ArrayList<>();
    if (!searchEnabled()) {
      return result;
    }
    String restNode = searchNodes().get(0);
    REST client = new REST(restNode, username, password, useSsl, searchPort).enableDebug(enableDebug);
    try {
      String endpoint = "api/index";
      JsonNode results = client.get(endpoint).validate().json();
      if (results.has("indexDefs") && !results.get("indexDefs").isNull()) {
        Iterator<Map.Entry<String, JsonNode>> indexes = results.get("indexDefs").get("indexDefs").fields();
        Stream<Map.Entry<String, JsonNode>> stream = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(indexes, 0), false);
        List<JsonNode> indexList = stream.map(Map.Entry::getValue).filter(JsonNode::isObject).collect(Collectors.toList());
        for (JsonNode index : indexList) {
          if (!index.get("sourceName").asText().equals(bucket)) {
            continue;
          }
          String bucketName = index.get("sourceName").asText();
          String scopeName = "_default";
          String indexName = index.get("name").asText();
          SearchIndexData i = new SearchIndexData();
          i.setName(indexName);
          i.setBucket(bucketName);
          i.setScope(scopeName);
          i.setConfig(index);
          result.add(i);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public List<IndexData> getIndexes(String bucket, String password) {
    connectBucket(bucket, password);
    List<JsonObject> indexes = query("SELECT * FROM system:indexes;");
    List<IndexData> result = new ArrayList<>();
    for (JsonObject index : indexes) {
      if (!index.get("keyspace_id").toString().equals(bucket)) {
        continue;
      }
      if (index.containsKey("using") && !index.get("using").toString().equals("gsi")) {
        continue;
      }
      if (index.containsKey("is_primary") && index.get("is_primary").toString().equals("true")) {
        IndexData i = new IndexData();
        i.setTable(index.get("keyspace_id").toString());
        i.setNumReplicas(-1);
        i.setPrimary(true);
        result.add(i);
      } else {
        IndexData i = new IndexData();
        i.setIndexKeys(getStringList((JsonArray) index.get("index_key")));
        i.setTable(index.get("keyspace_id").toString());
        i.setNumReplicas(-1);
        i.setName(index.get("name").toString());
        i.setCondition(index.containsKey("condition") ? index.get("condition").toString() : "");
        result.add(i);
      }
    }
    return result;
  }

  public List<TableData> getBuckets() {
    REST client = new REST(hostname, username, password, useSsl, adminPort).enableDebug(enableDebug);
    List<TableData> result = new ArrayList<>();
    for (String bucket : listBuckets()) {
      try {
        String endpoint = "pools/default/buckets/" + bucket;
        JsonNode bucketJson = client.get(endpoint).validate().json();
        BucketData b = new BucketData();
        b.setName(bucketJson.get("name").asText());
        b.setType(bucketJson.get("bucketType").asText());
        b.setQuota(bucketJson.get("quota").asInt() / 1048576);
        b.setReplicas(bucketJson.get("replicaNumber").asInt());
        b.setEviction(bucketJson.get("evictionPolicy").asText());
        b.setTtl(bucketJson.has("maxTTL") ? bucketJson.get("maxTTL").asInt() : 0);
        b.setStorage(bucketJson.has("storageBackend") ? bucketJson.get("storageBackend").asText() : "couchstore");
        b.setResolution(bucketJson.has("conflictResolutionType") ? bucketJson.get("conflictResolutionType").asText() : "seqno");
        if (majorRevision < 5) {
          b.setPassword(bucketJson.has("saslPassword") ? bucketJson.get("saslPassword").asText() : "");
        } else {
          b.setPassword("");
        }
        TableData t = new TableData();
        t.setName(bucket);
        t.setBucket(b);
        ScopeData s = new ScopeData();
        s.setName("_default");
        CollectionData c = new CollectionData();
        c.setName("_default");
        t.setScope(s);
        t.setCollection(c);
        t.setIndexes(getIndexes(b.getName(), b.getPassword()));
        t.setSearchIndexes(getSearchIndexes(bucketName));
        result.add(t);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  public RoleData parseRole(JsonNode role) {
    RoleData r = new RoleData();
    r.setRole(role.get("role").asText());
    r.setBucketName(role.hasNonNull("bucket_name") ? role.get("bucket_name").asText() : "*");
    r.setScopeName(role.hasNonNull("scope_name") ? role.get("scope_name").asText() : "*");
    r.setCollectionName(role.hasNonNull("collection_name") ? role.get("collection_name").asText() : "*");
    return r;
  }

  public List<UserData> getUsers() {
    if (majorRevision < 5) {
      return new ArrayList<>();
    }
    REST client = new REST(hostname, username, password, useSsl, adminPort).enableDebug(enableDebug);
    List<UserData> result = new ArrayList<>();
    try {
      String endpoint = "settings/rbac/users";
      JsonNode results = client.get(endpoint).validate().json();
      for (JsonNode user : results) {
        boolean local = user.has("domain") && user.get("domain").asText().equals("local");
        if (local) {
          UserData u = new UserData();
          u.setId(user.get("id").asText());
          u.setName(user.get("name").asText());
          u.setRoles(new ArrayList<>());
          u.setGroups(new ArrayList<>());
          if (user.has("roles")) {
            for (JsonNode role : user.get("roles")) {
              u.getRoles().add(parseRole(role));
            }
          }
          if (user.has("groups")) {
            for (JsonNode group : user.get("groups")) {
              u.getGroups().add(group.asText());
            }
          }
          result.add(u);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public List<GroupData> getGroups() {
    if (majorRevision < 6) {
      if (minorRevision < 5) {
        return new ArrayList<>();
      }
    }
    REST client = new REST(hostname, username, password, useSsl, adminPort);
    List<GroupData> result = new ArrayList<>();
    try {
      String endpoint = "settings/rbac/groups";
      JsonNode results = client.get(endpoint).validate().json();
      for (JsonNode group : results) {
        boolean ldap = group.has("ldap_group_ref") && !group.get("ldap_group_ref").isEmpty();
        if (!ldap) {
          GroupData g = new GroupData();
          g.setId(group.get("id").asText());
          g.setDescription(group.get("description").asText());
          g.setRoles(new ArrayList<>());
          if (group.has("roles")) {
            for (JsonNode role : group.get("roles")) {
              g.getRoles().add(parseRole(role));
            }
          }
          result.add(g);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
