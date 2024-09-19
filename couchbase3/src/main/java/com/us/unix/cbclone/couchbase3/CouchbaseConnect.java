package com.us.unix.cbclone.couchbase3;

import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.*;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.java.http.HttpPath;
import com.couchbase.client.java.http.HttpResponse;
import com.couchbase.client.java.http.HttpTarget;
import com.couchbase.client.java.manager.bucket.*;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.manager.query.CollectionQueryIndexManager;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.search.SearchIndexManager;
import com.couchbase.client.java.manager.user.Group;
import com.couchbase.client.java.manager.user.Role;
import com.couchbase.client.java.manager.user.User;
import com.couchbase.client.java.manager.user.UserManager;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.us.unix.cbclone.core.*;
import static com.us.unix.cbclone.core.RetryLogic.retryVoid;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Couchbase Connection Utility.
 */
public final class CouchbaseConnect {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseConnect.class);
  private volatile Cluster cluster;
  private volatile Bucket bucket;
  private volatile Scope scope;
  private volatile Collection collection;
  private volatile ClusterEnvironment environment;
  private volatile BucketManager bucketMgr;
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
  private int bucketReplicas;
  private final BucketType bucketType;
  private final StorageBackend bucketStorage;
  private final String rootCert;
  private final String clientCert;
  private final KeyStoreType keyStoreType;
  private String project;
  private String database;
  private boolean external;
  private String bucketName;
  private String scopeName;
  private String collectionName;
  private final Boolean useSsl;
  public int adminPort;
  public int eventingPort;
  private final int ttlSeconds;
  private static int maxParallelism;
  private final ObjectMapper mapper = new ObjectMapper();
  private JsonNode clusterInfo = mapper.createObjectNode();
  public String clusterVersion;
  public int majorRevision;
  public int minorRevision;
  public int patchRevision;
  public int buildNumber;
  public String clusterEdition;
  private final boolean enableDebug;
  private final ArrayNode hostMap = mapper.createArrayNode();

  /**
   * Builder Class.
   */
  public static class CouchbaseBuilder {
    private String hostname = DEFAULT_HOSTNAME;
    private String username = DEFAULT_USER;
    private String password = DEFAULT_PASSWORD;
    private String rootCert;
    private String clientCert;
    private KeyStoreType keyStoreType = KeyStoreType.PKCS12;
    private Boolean sslMode = DEFAULT_SSL_MODE;
    private Boolean enableDebug = false;
    private String bucketName;
    private int bucketReplicas = 1;
    private BucketType bucketType = BucketType.COUCHBASE;
    private StorageBackend bucketStorage = StorageBackend.COUCHSTORE;
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

    public CouchbaseBuilder bucketReplicas(final int count) {
      this.bucketReplicas = count;
      return this;
    }

    public CouchbaseBuilder bucketType(final BucketType type) {
      this.bucketType = type;
      return this;
    }

    public CouchbaseBuilder bucketStorage(final StorageBackend type) {
      this.bucketStorage = type;
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

    public CouchbaseBuilder keyStoreType(final KeyStoreType type) {
      this.keyStoreType = type;
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
    rootCert = builder.rootCert;
    clientCert = builder.clientCert;
    keyStoreType = builder.keyStoreType;
    enableDebug = builder.enableDebug;
    useSsl = builder.sslMode;
    ttlSeconds = builder.ttlSeconds;
    bucketName = builder.bucketName;
    bucketReplicas = builder.bucketReplicas;
    bucketType = builder.bucketType;
    bucketStorage = builder.bucketStorage;
    maxParallelism = 0;

    if (enableDebug) {
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      Configuration config = ctx.getConfiguration();
      LoggerConfig loggerConfig = config.getLoggerConfig(CouchbaseConnect.class.getName());
      loggerConfig.setLevel(Level.DEBUG);
      ctx.updateLoggers();
    }

    connect();
  }

  public void connect() {
    String couchbasePrefix;

    if (useSsl) {
      couchbasePrefix = "couchbases://";
      adminPort = 18091;
    } else {
      couchbasePrefix = "couchbase://";
      adminPort = 8091;
    }

    String connectString = couchbasePrefix + hostname;

    synchronized (STARTUP_COORDINATOR) {
      try {
        if (environment == null) {
          Consumer<SecurityConfig.Builder> secConfiguration;
          if (rootCert != null) {
            secConfiguration = securityConfig -> securityConfig
                .enableTls(true)
                .trustCertificate(Paths.get(rootCert));
          } else {
            secConfiguration = securityConfig -> securityConfig
                .enableTls(useSsl)
                .enableHostnameVerification(false)
                .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
          }

          Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> ioConfig
              .numKvConnections(4)
              .networkResolution(NetworkResolution.AUTO)
              .enableMutationTokens(false);

          Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> timeoutConfig
              .kvTimeout(Duration.ofSeconds(5))
              .connectTimeout(Duration.ofSeconds(15))
              .queryTimeout(Duration.ofSeconds(75));

          Authenticator authenticator;
          if (clientCert != null) {
            KeyStore keyStore = KeyStore.getInstance(keyStoreType.name());
            keyStore.load(new FileInputStream(clientCert), password.toCharArray());
            authenticator = CertificateAuthenticator.fromKeyStore(
                keyStore,
                password
            );
          } else {
            authenticator = PasswordAuthenticator.create(username, password);
          }

          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(timeOutConfiguration)
              .ioConfig(ioConfiguration)
              .securityConfig(secConfiguration)
              .build();
          cluster = Cluster.connect(connectString,
              ClusterOptions.clusterOptions(authenticator).environment(environment));
          cluster.waitUntilReady(Duration.ofSeconds(15));
          bucketMgr = cluster.buckets();
          try {
            if (bucketName != null) {
              bucketMgr.getBucket(bucketName);
              bucket = cluster.bucket(bucketName);
            }
          } catch (BucketNotFoundException ignored) { }
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
    clusterInfo = mapper.createObjectNode();
  }

  public CouchbaseStream stream(String bucketName) {
    return new CouchbaseStream(hostname, username, password, bucketName, true);
  }

  public CouchbaseStream stream(String bucketName, String scopeName, String collectionName) {
    return new CouchbaseStream(hostname, username, password, bucketName, true, scopeName, collectionName);
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
    HttpResponse response = cluster.httpClient().get(
        HttpTarget.manager(),
        HttpPath.of("/pools/default"));
    try {
      clusterInfo = mapper.readTree(response.contentAsString());
      String clusterFullVersion = clusterInfo.get("nodes").get(0).get("version").asText();
      clusterVersion = clusterFullVersion.split("-")[0];
      buildNumber = Integer.parseInt(clusterFullVersion.split("-")[1]);
      clusterEdition = clusterFullVersion.split("-")[2];
      majorRevision = Integer.parseInt(clusterVersion.split("\\.")[0]);
      minorRevision = Integer.parseInt(clusterVersion.split("\\.")[1]);
      patchRevision = Integer.parseInt(clusterVersion.split("\\.")[2]);

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
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public long getIndexNodeCount() {
    Stream<JsonNode> stream = StreamSupport.stream(hostMap.spliterator(), false);
    return stream.filter(e -> {
      try {
        List<String> services = mapper.readerForListOf(String.class).readValue(e.get("services"));
        return services.contains("index");
      } catch (IOException ex) {
        return false;
      }
    }).count();
  }

  private int getMemQuota() {
    int used = (int) (clusterInfo.get("storageTotals").get("ram").get("quotaUsedPerNode").asLong() / 1048576);
    int total = (int) (clusterInfo.get("storageTotals").get("ram").get("quotaTotalPerNode").asLong() / 1048576);
    return total - used;
  }

  public List<String> listBuckets() {
    return new ArrayList<>(cluster.buckets().getAllBuckets().keySet());
  }

  public Boolean isBucket(String bucket) {
    List<String> results = listBuckets();
    return results.contains(bucket);
  }

  public void connectBucket() {
    bucket = cluster.bucket(bucketName);
  }

  public void connectBucket(String name) {
    this.bucketName = name;
    bucket = cluster.bucket(bucketName);
  }

  public void connectScope(String scopeName) {
    this.scopeName = scopeName;
    scope = bucket.scope(scopeName);
  }

  public void connectCollection(String collectionName) {
    this.collectionName = collectionName;
    collection = scope.collection(collectionName);
  }

  public ReactiveCollection reactiveCollection() {
    if (collection == null) {
      throw new RuntimeException("Collection is not connected");
    }
    return collection.reactive();
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

  public void createBucket(BucketData bucketData) {
    switch (bucketData.getType()) {
      case "membase":
        bucketData.setType("COUCHBASE");
        break;
      case "ephemeral":
        bucketData.setType("EPHEMERAL");
        break;
      case "memcached":
        bucketData.setType("MEMCACHED");
        break;
    }
    switch (bucketData.getResolution()) {
      case "seqno":
        bucketData.setResolution("SEQUENCE_NUMBER");
        break;
      case "lww":
        bucketData.setResolution("TIMESTAMP");
        break;
      case "custom":
        bucketData.setResolution("CUSTOM");
        break;
    }
    if (bucketData.getQuota() == 0) {
      bucketData.setQuota(128);
    }
    BucketSettings bucketSettings = BucketSettings.create(bucketData.getName())
        .flushEnabled(false)
        .replicaIndexes(true)
        .ramQuotaMB(bucketData.getQuota())
        .numReplicas(bucketData.getReplicas())
        .bucketType(BucketType.valueOf(bucketData.getType()))
        .storageBackend(StorageBackend.of(bucketData.getStorage()))
        .conflictResolutionType(ConflictResolutionType.valueOf(bucketData.getResolution()));
    try {
      bucketMgr.createBucket(bucketSettings);
    } catch (BucketExistsException e) {
      LOGGER.debug("createBucket: Bucket {} already exists", bucketData.getName());
    }
  }

  public void bucketCreate(String name, int quota) {
    BucketSettings bucketSettings = BucketSettings.create(name)
        .flushEnabled(false)
        .replicaIndexes(true)
        .ramQuotaMB(quota)
        .numReplicas(bucketReplicas)
        .bucketType(bucketType)
        .storageBackend(bucketStorage)
        .conflictResolutionType(ConflictResolutionType.SEQUENCE_NUMBER);
    try {
      bucketMgr.createBucket(bucketSettings);
    } catch (BucketExistsException e) {
      LOGGER.debug("bucketCreate: Bucket {} already exists", name);
    }
  }

  public void dropBucket(String name) {
    try {
      bucketMgr.dropBucket(name);
    } catch (BucketNotFoundException e) {
      LOGGER.debug("Drop: Bucket {} does not exist", name);
    }
  }

  public void createScope(String bucketName, String scopeName) {
    if (Objects.equals(scopeName, "_default")) {
      return;
    }
    bucketMgr.getBucket(bucketName);
    bucket = cluster.bucket(bucketName);
    CollectionManager collectionManager = bucket.collections();
    try {
      collectionManager.createScope(scopeName);
    } catch (ScopeExistsException e) {
      LOGGER.debug("Scope {} already exists in cluster", scopeName);
    }
  }

  public void createCollection(String bucketName, String scopeName, String collectionName) {
    if (Objects.equals(collectionName, "_default")) {
      return;
    }
    bucketMgr.getBucket(bucketName);
    bucket = cluster.bucket(bucketName);
    CollectionManager collectionManager = bucket.collections();
    try {
      collectionManager.createCollection(scopeName, collectionName);
    } catch (CollectionExistsException e) {
      LOGGER.debug("Collection {} already exists in cluster", collectionName);
    }
  }

  public boolean collectionExists(String bucketName, String scopeName, String collectionName) {
    Bucket bucket = cluster.bucket(bucketName);
    try {
      Scope scope = bucket.scope(scopeName);
      scope.collection(collectionName);
      return true;
    } catch (CollectionNotFoundException e) {
      return false;
    }
  }

  public void createPrimaryIndex(String bucketName, String scopeName, String collectionName, int replicaCount) {
    Bucket bucket = cluster.bucket(bucketName);
    Scope scope = bucket.scope(scopeName);
    Collection collection = scope.collection(collectionName);

    CollectionQueryIndexManager queryIndexMgr = collection.queryIndexes();
    CreatePrimaryQueryIndexOptions options = CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions()
        .deferred(false)
        .numReplicas(replicaCount)
        .ignoreIfExists(true);

    queryIndexMgr.createPrimaryIndex(options);
  }

  public void createSecondaryIndex(String bucketName, String scopeName, String collectionName, String indexName,
                                   List<String> indexKeys, int replicaCount) {
    Bucket bucket = cluster.bucket(bucketName);
    Scope scope = bucket.scope(scopeName);
    Collection collection = scope.collection(collectionName);

    CollectionQueryIndexManager queryIndexMgr = collection.queryIndexes();
    CreateQueryIndexOptions options = CreateQueryIndexOptions.createQueryIndexOptions()
        .deferred(false)
        .numReplicas(replicaCount)
        .ignoreIfExists(true);

    LOGGER.debug("Creating GSI: {} {} {}", indexName, indexKeys, options);
    queryIndexMgr.createIndex(indexName, indexKeys, options);
    queryIndexMgr.watchIndexes(Collections.singletonList(indexName), Duration.ofSeconds(10));
  }

  public void createSearchIndex(JsonNode config) {
    if (cluster == null) {
      throw new RuntimeException("Cluster is not connected");
    }
    SearchIndexManager search = cluster.searchIndexes();
    try {
      search.getIndex(config.get("name").asText());
    } catch (IndexNotFoundException e) {
      SearchIndex index = SearchIndex.fromJson(config.toString());
      search.upsertIndex(index);
    }
  }

  public Set<Role> defaultRoles() {
    return new HashSet<>(List.of(
        new Role("data_reader", "*"),
        new Role("query_select", "*"),
        new Role("data_writer", "*"),
        new Role("query_insert", "*"),
        new Role("query_delete", "*"),
        new Role("query_manage_index", "*")
    ));
  }

  public Set<Role> constructRoles(List<RoleData> roles) {
    if (roles.isEmpty()) {
      return defaultRoles();
    } else {
      Set<Role> roleList = new HashSet<>();
      for (RoleData roleData : roles) {
        Role role;
        if (!roleData.getScopeName().equals("*") || !roleData.getCollectionName().equals("*")) {
          role = new Role(roleData.getRole(),
              roleData.getBucketName(),
              roleData.getScopeName(),
              roleData.getCollectionName());
        } else if (!roleData.getBucketName().equals("*")) {
          role = new Role(roleData.getRole(), roleData.getBucketName());
        } else {
          role = new Role(roleData.getRole());
        }
        roleList.add(role);
      }
      return roleList;
    }
  }

  public void createUser(String userName, String passWord, String fullName, List<String> groups, List<RoleData> roles) {
    UserManager um = cluster.users();
    User user = new User(userName);
    if (!groups.isEmpty()) {
      user.groups(groups);
    }
    user.roles(constructRoles(roles));
    if (passWord != null && !passWord.isEmpty()) {
      user.password(passWord);
    } else {
      user.password(password);
    }
    if (fullName != null && !fullName.isEmpty()) {
      user.displayName(fullName);
    }
    LOGGER.debug("Creating user {}", user);
    um.upsertUser(user);
  }

  public void createGroup(String groupName, String description, List<RoleData> roles) {
    UserManager um = cluster.users();
    Group group = new Group(groupName);
    if (description != null && !description.isEmpty()) {
      group.description(description);
    }
    group.roles(constructRoles(roles));
    LOGGER.debug("Creating group {}", group);
    um.upsertGroup(group);
  }

  public JsonNode get(String id) {
    if (collection == null) {
      throw new RuntimeException("Collection is not connected");
    }
    try {
      String result = collection.get(id, getOptions().transcoder(RawJsonTranscoder.INSTANCE)).contentAs(String.class);
      return mapper.readTree(result);
    } catch (DocumentNotFoundException e) {
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void upsert(String id, Object content) {
    if (collection == null) {
      throw new RuntimeException("Collection is not connected");
    }
    try {
      collection.upsert(id, content, upsertOptions().expiry(Duration.ofSeconds(ttlSeconds)).timeout(Duration.ofSeconds(5)));
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public List<JsonNode> query(String queryString) {
    if (cluster == null) {
      throw new RuntimeException("Bucket is not connected");
    }
    TypeRef<Map<String, Object>> typeRef = new TypeRef<>() {};
    try {
      return cluster.reactive().query(queryString, queryOptions()
              .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
              .maxParallelism(maxParallelism))
          .flatMapMany(res -> res.rowsAs(typeRef))
          .map(Map::values)
          .flatMapIterable(o -> mapper.convertValue(o, JsonNode.class))
          .collectList()
          .block();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public List<String> getStringList(JsonNode node) {
    try {
      return mapper.readerFor(new TypeReference<List<String>>() {}).readValue(node);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<SearchIndexData> getSearchIndexes(String bucket, String scope) {
    List<SearchIndexData> result = new ArrayList<>();
    try {
      SearchIndexManager search = cluster.searchIndexes();
      for (SearchIndex index : search.getAllIndexes()) {
        if (!index.sourceName().equals(bucket)) {
          continue;
        }
        String bucketName = index.name().split("\\.")[0];
        String scopeName = index.name().split("\\.")[1];
        String indexName = index.name().split("\\.")[2];
        if (!scopeName.equals(scope)) {
          continue;
        }
        SearchIndexData i = new SearchIndexData();
        i.setName(indexName);
        i.setBucket(bucketName);
        i.setScope(scopeName);
        i.setConfig(mapper.readTree(index.toJson()));
        result.add(i);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } catch (ServiceNotAvailableException e) {
      LOGGER.debug("Search service is not configured in the cluster");
    }
    return result;
  }

  public List<IndexData> getIndexes(String bucket, String collection) {
    List<JsonNode> indexes = query("SELECT * FROM system:indexes;");
    List<IndexData> result = new ArrayList<>();
    int replicas = -1;
    for (JsonNode index : indexes) {
      if (collection.equals("_default")) {
        if (!index.get("keyspace_id").asText().equals(bucket)) {
          continue;
        }
      } else {
        if (!index.get("keyspace_id").asText().equals(collection)) {
          continue;
        }
      }
      if (index.has("using") && !index.get("using").asText().equals("gsi")) {
        continue;
      }
      if (index.has("metadata")) {
        if (index.get("metadata").has("num_replica")) {
          replicas = index.get("metadata").get("num_replica").asInt();
        }
      }
      if (index.has("is_primary") && index.get("is_primary").asBoolean()) {
        IndexData i = new IndexData();
        i.setTable(index.get("keyspace_id").asText());
        i.setName(index.get("name").asText());
        i.setNumReplicas(replicas);
        i.setPrimary(true);
        result.add(i);
      } else {
        IndexData i = new IndexData();
        i.setIndexKeys(getStringList(index.get("index_key")));
        i.setTable(index.get("keyspace_id").asText());
        i.setName(index.get("name").asText());
        i.setNumReplicas(replicas);
        i.setCondition(index.has("condition") ? index.get("condition").asText() : "");
        result.add(i);
      }
    }
    return result;
  }

  public List<TableData> getBuckets() {
    List<TableData> result = new ArrayList<>();
    for (Map.Entry<String, BucketSettings> entry : cluster.buckets().getAllBuckets().entrySet()) {
      BucketSettings bucketSettings = entry.getValue();
      String bucketName = bucketSettings.name();
      Bucket bucket = cluster.bucket(bucketName);
      CollectionManager cm = bucket.collections();
      for (ScopeSpec scope : cm.getAllScopes()) {
        String scopeName = scope.name();
        if (scopeName.equals("_system")) {
          continue;
        }
        for (CollectionSpec collection : scope.collections()) {
          String collectionName = collection.name();
          try {
            BucketData b = new BucketData();
            b.setName(bucketName);
            b.setType(bucketSettings.bucketType().toString());
            b.setQuota((int) bucketSettings.ramQuotaMB());
            b.setReplicas(bucketSettings.numReplicas());
            b.setEviction(bucketSettings.evictionPolicy().toString());
            b.setTtl((int) bucketSettings.maxExpiry().getSeconds());
            b.setStorage(bucketSettings.storageBackend().toString());
            b.setResolution(bucketSettings.conflictResolutionType().toString());
            b.setPassword("");
            TableData t = new TableData();
            t.setName(bucketSettings.name());
            t.setBucket(b);
            ScopeData s = new ScopeData();
            s.setName(scopeName);
            CollectionData c = new CollectionData();
            c.setName(collectionName);
            c.setTtl((int) collection.maxExpiry().getSeconds());
            c.setHistory(collection.history() != null ? collection.history() : false);
            t.setScope(s);
            t.setCollection(c);
            t.setIndexes(getIndexes(bucketName, collectionName));
            t.setSearchIndexes(getSearchIndexes(bucketName, scopeName));
            result.add(t);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return result;
  }

  public RoleData parseRole(JsonNode role) {
    RoleData r = new RoleData();
    LOGGER.debug(role.toPrettyString());
    r.setRole(role.get("role").asText());
    r.setBucketName(role.has("bucket_name") ? role.get("bucket_name").asText() : "*");
    r.setScopeName(role.has("scope_name") ? role.get("scope_name").asText() : "*");
    r.setCollectionName(role.has("collection_name") ? role.get("collection_name").asText() : "*");
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

  public void createBuckets(List<TableData> buckets) {
    for (TableData bucket : buckets) {
      String bucketName = bucket.getName();
      String scopeName = bucket.getScope().getName();
      String collectionName = bucket.getCollection().getName();

      // Create bucket
      LOGGER.info("Creating bucket {}", bucketName);
      createBucket(bucket.getBucket());

      // Create scope
      if (!Objects.equals(scopeName, "_default")) {
        LOGGER.info("Creating scope {}.{}", bucketName, scopeName);
        createScope(bucketName, scopeName);
      }

      // Create collection
      if (!Objects.equals(collectionName, "_default")) {
        LOGGER.info("Creating collection {}.{}.{}", bucketName, scopeName, collectionName);
        createCollection(bucketName, scopeName, collectionName);
      }

      // Create indexes
      for (IndexData index : bucket.getIndexes()) {
        int replicas = index.getNumReplicas();
        if (replicas < 0) {
          replicas = (int) getIndexNodeCount() - 1;
        }
        final int replicaNum = replicas;
        try {
          LOGGER.info("{} {} {} {} {} {}", bucketName, scopeName, collectionName, index.getName(), index.getIndexKeys(), replicaNum);
          if (index.isPrimary()) {
            LOGGER.info("Creating primary index on keyspace {}.{}.{}", bucketName, scopeName, collectionName);
            retryVoid(() -> createPrimaryIndex(bucketName, scopeName, collectionName, replicaNum));
          } else {
            LOGGER.info("Creating secondary index {} on keyspace {}.{}.{}", index.getName(), bucketName, scopeName, collectionName);
            retryVoid(() -> createSecondaryIndex(bucketName, scopeName, collectionName, index.getName(), index.getIndexKeys(), replicaNum));
          }
        } catch (Exception e) {
          throw new RuntimeException("Index creation failed: " + e.getMessage(), e);
        }
      }

      // Create Search Indexes
      for (SearchIndexData searchIndex : bucket.getSearchIndexes()) {
        LOGGER.info("Creating search index {}", searchIndex.getName());
        ObjectNode config = searchIndex.getConfig().deepCopy();
        if (config.has("sourceUUID")) {
          config.remove("sourceUUID");
        }
        if (config.has("uuid")) {
          config.remove("uuid");
        }
        config.put("name", searchIndex.getName());
        LOGGER.debug("Search Index config:\n{}", searchIndex.getConfig().toPrettyString());
        createSearchIndex(config);
      }
    }
  }
}
