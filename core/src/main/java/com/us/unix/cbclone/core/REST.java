package com.us.unix.cbclone.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.us.unix.cbclone.core.exceptions.*;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;

/**
 * Connect To REST Interface.
 */
public class REST {
  static final Logger LOGGER = LoggerFactory.getLogger(REST.class);
  private final String hostname;
  private String username;
  private String password;
  private String token = null;
  private final Boolean useSsl;
  private final Integer port;
  private final OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
  private OkHttpClient client;
  private String credential;
  private boolean enableDebug;
  private final ObjectMapper mapper = new ObjectMapper();
  public int responseCode;
  public byte[] responseBody;
  public RequestBody requestBody;
  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private int successStart = 200;
  private int successEnd = 299;
  private int permissionDeniedCode = 403;
  private int notFoundCode = 404;
  private int rateLimitCode = 429;
  private int serverErrorCode = 500;

  public REST(String hostname, String username, String password, Boolean useSsl) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = useSsl;
    this.port = useSsl ? 443 : 80;
    this.enableDebug = false;
    this.init();
  }

  public REST(String hostname, String username, String password, Boolean useSsl, Integer port) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = useSsl;
    this.port = port;
    this.enableDebug = false;
    this.init();
  }

  public REST(String hostname, String token, Boolean useSsl) {
    this.hostname = hostname;
    this.token = token;
    this.useSsl = useSsl;
    this.enableDebug = false;
    this.port = useSsl ? 443 : 80;
    this.init();
  }

  public void init() {
    TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] chain, String authType) {
          }

          @Override
          public void checkServerTrusted(X509Certificate[] chain, String authType) {
          }

          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[]{};
          }
        }
    };

    SSLContext sslContext;
    try {
      sslContext = SSLContext.getInstance("SSL");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    try {
      sslContext.init(null, trustAllCerts, new SecureRandom());
    } catch (KeyManagementException e) {
      throw new RuntimeException(e);
    }

    clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0]);
    clientBuilder.hostnameVerifier((hostname, session) -> true);
    clientBuilder.connectTimeout(Duration.ofSeconds(20));
    clientBuilder.readTimeout(Duration.ofSeconds(20));
    clientBuilder.writeTimeout(Duration.ofSeconds(20));

    if (token != null) {
      credential = "Bearer " + token;
    } else {
      credential = Credentials.basic(username, password);
    }

    if (enableDebug) {
      HttpLoggingInterceptor logging = new HttpLoggingInterceptor();
      logging.setLevel(HttpLoggingInterceptor.Level.HEADERS);
      clientBuilder.addInterceptor(logging);
    }

    client = clientBuilder.build();
  }

  public REST enableDebug(boolean value) {
    this.enableDebug = value;
    return this;
  }

  public void setSuccessRange(int start, int end) {
    this.successStart = start;
    this.successEnd = end;
  }

  public void setPermissionDeniedCode(int code) {
    this.permissionDeniedCode = code;
  }

  public void setNotFoundCode(int code) {
    this.notFoundCode = code;
  }

  public void setRateLimitCode(int code) {
    this.rateLimitCode = code;
  }

  public void setServerErrorCode(int code) {
    this.serverErrorCode = code;
  }

  private void execHttpCall(Request request) {
    try {
      LOGGER.debug("Request: {}", request.url());
      try (Response response = client.newCall(request).execute()) {
        responseCode = response.code();
        responseBody = response.body() != null ? response.body().bytes() : new byte[0];
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public REST get(String endpoint) {
    execHttpCall(buildGetRequest(endpoint));
    return this;
  }

  public REST post(String endpoint) {
    execHttpCall(buildPostRequest(endpoint, requestBody));
    return this;
  }

  public REST delete(String endpoint) {
    execHttpCall(buildDeleteRequest(endpoint));
    return this;
  }

  public JsonNode json() {
    try {
      return mapper.readTree(new String(responseBody));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public ArrayNode jsonArray() {
    try {
      return (ArrayNode) mapper.readTree(new String(responseBody));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> jsonSearch(String key) {
    return json().findValuesAsText(key);
  }

  public REST jsonBody(JsonNode json) {
    requestBody = RequestBody.create(json.toString(), JSON);
    return this;
  }

  public int code() {
    return responseCode;
  }

  public REST validate() {
    if (responseCode >= 300) {
      String response = new String(responseBody);
      throw new RuntimeException(
          "Invalid response from API endpoint: response code: " + responseCode + " Response: " + response
      );
    }
    return this;
  }

  private REST validateResponse() throws HttpResponseException {
    if (this.responseCode >= successStart && this.responseCode < successEnd) {
      return this;
    } else if (this.responseCode == permissionDeniedCode) {
      throw new PermissionDeniedError(new String(responseBody));
    } else if (this.responseCode == notFoundCode) {
      throw new NotFoundError(new String(responseBody));
    } else if (this.responseCode == rateLimitCode) {
      throw new RateLimitError(new String(responseBody));
    } else if (this.responseCode == serverErrorCode) {
      throw new InternalServerError(new String(responseBody));
    } else if (this.responseCode >= 400 && this.responseCode < 500) {
      throw new RetryableError(String.format("code: %d response: %s", this.responseCode, new String(responseBody)));
    } else {
      throw new NonRetryableError(String.format("code: %d response: %s", this.responseCode, new String(responseBody)));
    }
  }

  public boolean waitForJsonValue(String endpoint, String key, String value, int retryCount) {
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      JsonNode response = get(endpoint).validate().json();
      String result = response.get(key).toString();
      if (result.equals(value)) {
        return true;
      }
      try {
        Thread.sleep(waitFactor);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  public boolean waitForCode(String endpoint, int code, int retryCount) {
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      int result = get(endpoint).code();
      if (result == code) {
        return true;
      }
      try {
        Thread.sleep(waitFactor);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  public HttpUrl buildUrl(String endpoint) {
    HttpUrl.Builder builder = new HttpUrl.Builder();
    return builder.scheme(useSsl ? "https" : "http")
        .host(hostname)
        .port(port)
        .addPathSegments(endpoint)
        .build();
  }

  public Request buildGetRequest(String endpoint) {
    HttpUrl url = buildUrl(endpoint);
    return new Request.Builder()
        .url(url)
        .header("Authorization", credential)
        .build();
  }

  public Request buildPostRequest(String endpoint, RequestBody body) {
    HttpUrl url = buildUrl(endpoint);
    return new Request.Builder()
        .url(url)
        .post(body)
        .header("Authorization", credential)
        .build();
  }

  public Request buildDeleteRequest(String endpoint) {
    HttpUrl url = buildUrl(endpoint);
    return new Request.Builder()
        .url(url)
        .delete()
        .header("Authorization", credential)
        .build();
  }
}
