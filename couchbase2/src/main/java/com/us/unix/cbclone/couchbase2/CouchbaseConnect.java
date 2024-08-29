package com.us.unix.cbclone.couchbase2;

import com.couchbase.client.core.env.*;
import com.couchbase.client.java.*;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.BucketAlreadyExistsException;
import com.couchbase.client.java.error.BucketDoesNotExistException;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.util.features.Version;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import com.us.unix.cbclone.REST;

/**
 * Couchbase Connection Utility.
 */
public final class CouchbaseConnect {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseConnect.class);
  private volatile Cluster cluster;
  private volatile ClusterManager clusterManager;
  private volatile Bucket bucket;
  public static final String DEFAULT_ADMIN_USER = "Administrator";
  public static final String DEFAULT_ADMIN_PASSWORD = "password";
  public static final String DEFAULT_BUCKET_PASSWORD = "";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final Boolean DEFAULT_SSL_MODE = false;
  public static final String DEFAULT_SSL_SETTING = "true";
  private static final String DEFAULT_PROJECT = null;
  private static final String DEFAULT_DATABASE = null;
  private static final Object STARTUP_COORDINATOR = new Object();
  private final String hostname;
  private final String adminUsername;
  private final String adminPassword;
  private String bucketPassword;
  private int bucketReplicas;
  private final String rootCert;
  private final String clientCert;
  private String project;
  private String database;
  private boolean external;
  private String bucketName;
  private final Boolean useSsl;
  public int adminPort;
  public int eventingPort;
  private final int ttlSeconds;
  private final ObjectMapper mapper = new ObjectMapper();
  private JsonNode hostMap = mapper.createObjectNode();
  private JsonNode clusterInfo = mapper.createObjectNode();
  public String clusterVersion;

  /**
   * Builder Class.
   */
  public static class CouchbaseBuilder {
    private String hostName = DEFAULT_HOSTNAME;
    private String adminUser = DEFAULT_ADMIN_USER;
    private String adminPass = DEFAULT_ADMIN_PASSWORD;
    private String bucketPass = DEFAULT_BUCKET_PASSWORD;
    private String rootCert;
    private String clientCert;
    private Boolean sslMode = DEFAULT_SSL_MODE;
    private String bucketName;
    private int bucketReplicas = 1;
    private int ttlSeconds = 0;

    public CouchbaseBuilder ttl(int value) {
      this.ttlSeconds = value;
      return this;
    }

    public CouchbaseBuilder host(final String name) {
      this.hostName = name;
      return this;
    }

    public CouchbaseBuilder adminUser(final String name) {
      this.adminPass = name;
      return this;
    }

    public CouchbaseBuilder adminPassword(final String name) {
      this.adminPass = name;
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

    public CouchbaseBuilder connect(final String host, final String user, final String password) {
      this.hostName = host;
      this.adminUser = user;
      this.adminPass = password;
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

    public CouchbaseConnect build() {
      return new CouchbaseConnect(this);
    }
  }

  private CouchbaseConnect(CouchbaseBuilder builder) {
    hostname = builder.hostName;
    adminUsername = builder.adminUser;
    adminPassword = builder.adminPass;
    bucketPassword = builder.bucketPass;
    rootCert = builder.rootCert;
    clientCert = builder.clientCert;
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
    } else {
      couchbasePrefix = "couchbase://";
      adminPort = 8091;
    }

    String connectString = couchbasePrefix + hostname;

    synchronized (STARTUP_COORDINATOR) {
      try {
        if (cluster == null) {

          cluster = CouchbaseCluster.create(hostname);
          clusterManager = cluster.clusterManager(adminUsername, adminPassword);
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
    hostMap = mapper.createObjectNode();
    clusterInfo = mapper.createObjectNode();
  }

  public String hostValue() {
    return hostname;
  }

  public String adminUserValue() {
    return adminUsername;
  }

  public String adminPasswordValue() {
    return adminPassword;
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
    try {
      clusterInfo = mapper.readTree(clusterData.raw().toString());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private int getMemQuota() {
    int used = (int) (clusterInfo.get("storageTotals").get("ram").get("quotaUsedPerNode").asLong() / 1048576);
    int total = (int) (clusterInfo.get("storageTotals").get("ram").get("quotaTotalPerNode").asLong() / 1048576);
    return total - used;
  }

  public Boolean isBucket(String bucket) {
    REST client = new REST(hostname, adminUsername, adminPassword, useSsl, adminPort).enableDebug(true);
    try {
      String endpoint = "pools/default/buckets";
      List<String> results = client.get(endpoint).validate().json().findValuesAsText("name");
      return results.contains(bucket);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  public void getIndexes() {
    List<JsonObject> indexes = query("SELECT * FROM system:indexes;");
    System.out.println("Get Indexes");
    for (JsonObject index : indexes) {
      System.out.println(index.toString());
    }
  }
}
