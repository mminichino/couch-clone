package com.us.unix.cbclone.couchbase2;

import com.couchbase.client.dcp.*;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Couchbase Stream Utility.
 */
public class CouchbaseStream {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseStream.class);
  private final String hostname;
  private final String username;
  private final String password;
  private final String bucket;
  private final Boolean useSsl;
  private boolean collectionEnabled;
  private final AtomicLong totalSize = new AtomicLong(0);
  private final AtomicLong docCount = new AtomicLong(0);
  private final AtomicLong sentCount = new AtomicLong(0);
  private final PriorityBlockingQueue<String> queue = new PriorityBlockingQueue<>();
  private Client client;

  public CouchbaseStream(String hostname, String username, String password, String bucket, Boolean ssl) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.bucket = bucket;
    this.useSsl = ssl;
    this.init();
  }

  public void init() {
    StringBuilder connectBuilder = new StringBuilder();

    String couchbasePrefix;
    if (useSsl) {
      couchbasePrefix = "couchbases://";
    } else {
      couchbasePrefix = "couchbase://";
    }

    connectBuilder.append(couchbasePrefix);
    connectBuilder.append(hostname);

    String connectString = connectBuilder.toString();

    Client.Builder clientBuilder = Client.builder()
        .connectionString(connectString)
        .bucket(bucket)
        .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 1048576)
        .bufferAckWatermark(75);

    if (!username.isEmpty()) {
      clientBuilder.username(username);
    }

    if (!password.isEmpty()) {
      clientBuilder.password(password);
    }

    client = clientBuilder.build();

    client.controlEventHandler((flowController, event) -> {
      flowController.ack(event);
      event.release();
    });
  }

  public void toCompressedFile(String filename) throws IOException {
    try (FileOutputStream output = new FileOutputStream(filename)) {
      Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), StandardCharsets.UTF_8);
      streamDocuments();
      startToNow();
      streamData().forEach(record -> {
        try {
          writer.write(record + "\n");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      stop();
      writer.close();
    } catch (Exception e) {
      throw new IOException("Can not stream to file " + filename, e);
    }
  }

  public void streamDocuments() {
    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        String key = MessageUtil.getKeyAsString(event);
        byte[] content = DcpMutationMessage.contentBytes(event);
        try {
          ObjectMapper mapper = new ObjectMapper();
          JsonNode document = mapper.readTree(content);
          ObjectNode metadata = mapper.createObjectNode();
          metadata.put("id", key);
          ObjectNode root = mapper.createObjectNode();
          root.set("metadata", metadata);
          root.set("document", document);
          queue.add(root.toString());
          flowController.ack(event);
          docCount.incrementAndGet();
        } catch (Exception e) {
          LOGGER.error("Error reading stream: {}", e.getMessage(), e);
        }
      }
      event.release();
    });
  }

  public void startToNow() {
    client.connect().block();
    client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).block();
    client.startStreaming().block();
  }

  public void startFromNow() {
    client.connect().block();
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).block();
    client.startStreaming().block();
  }

  public <T> Stream<T> whileNotNull(Supplier<? extends T> supplier) {
    requireNonNull(supplier);
    return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.NONNULL) {
          @Override
          public boolean tryAdvance(Consumer<? super T> action) {
            do {
              T element = supplier.get();
              if (element != null) {
                action.accept(element);
                sentCount.incrementAndGet();
                return true;
              }
            } while (!client.sessionState().isAtEnd() || sentCount.get() < docCount.get());
            return false;
          }
        }, false);
  }

  public Stream<String> streamData() {
    return whileNotNull(queue::poll);
  }

  public Stream<String> getByCount(long count) {
    return Stream.generate(() -> {
          try {
            return queue.take();
          } catch (InterruptedException ex) {
            return null;
          }
        })
        .limit(count);
  }

  public void stop() {
    client.disconnect().block();
  }

  public long getCount() {
    return docCount.get();
  }
}
