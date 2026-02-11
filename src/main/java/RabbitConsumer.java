import com.rabbitmq.client.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import java.io.StringReader;

public class RabbitConsumer {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static HikariDataSource dataSource;

  private static final String INSERT_SQL = loadSql("/sql/insert_incoming_event.sql");

  // ----------- Java 8 helper (InputStream.readAllBytes replacement) -----------
  private static byte[] readAllBytesJava8(InputStream is) throws java.io.IOException {
    java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
    byte[] data = new byte[8192];
    int nRead;
    while ((nRead = is.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    return buffer.toByteArray();
  }

  private static String loadSql(String resourcePath) {
    try (InputStream is = RabbitConsumer.class.getResourceAsStream(resourcePath)) {
      if (is == null) throw new IllegalStateException("SQL resource not found: " + resourcePath);
      return new String(readAllBytesJava8(is), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load SQL: " + resourcePath, e);
    }
  }

  public static void main(String[] args) throws Exception {

    // ---- DB pool ----
    dataSource = buildDataSource();

    // ---- RabbitMQ connection ----
    String host = mustEnv("RABBIT_HOST");
    int port = Integer.parseInt(env("RABBIT_PORT", "5671"));
    String user = mustEnv("RABBIT_USERNAME");
    String pass = mustEnv("RABBIT_PASSWORD");
    String vhost = env("RABBIT_VHOST", "e-star-trades");
    String queue = env("RABBIT_QUEUE", "INTERNALMARKET.TRADES.INSTATUS");
    int prefetch = Integer.parseInt(env("RABBIT_PREFETCH", "50"));

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(user);
    factory.setPassword(pass);
    factory.setVirtualHost(vhost);

    // OpenShift-friendly settings (Java 8)
    factory.setAutomaticRecoveryEnabled(true);
    factory.setTopologyRecoveryEnabled(true);
    factory.setNetworkRecoveryInterval(5000); // ms
    factory.setRequestedHeartbeat(30);

    // TLS (AMQPS)
    factory.useSslProtocol();
    factory.enableHostnameVerification();

    final com.rabbitmq.client.Connection rmqConn = factory.newConnection("rabbit-consumer");
    final com.rabbitmq.client.Channel channel = rmqConn.createChannel();

    channel.basicQos(prefetch);

    // Ensure queue exists (will throw if not)
    channel.queueDeclarePassive(queue);
    System.out.println("Queue exists: " + queue);

    DeliverCallback onMessage = (consumerTag, delivery) -> {
      long tag = delivery.getEnvelope().getDeliveryTag();

      AMQP.BasicProperties props = delivery.getProperties();
  String receivedAt = ZonedDateTime.now()
      .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

  System.out.println("[" + receivedAt + "] Message received from RabbitMQ");

      // Declare eventKey OUTSIDE try so catch blocks can use it
      String eventKey = null;

      try {
        String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);

        // ---- Determine idempotency key ----

        eventKey = extractTradeIdFromXml(msg);

        System.out.println("Received tag=" + tag +
            " redelivered=" + delivery.getEnvelope().isRedeliver() +
            " eventKey=" + eventKey +
            " bytes=" + delivery.getBody().length);

        // ---- Persist FIRST, then ACK ----
        persistEvent(eventKey, msg);
        channel.basicAck(tag, false);

        System.out.println("Persisted + ACKed event_key=" + eventKey);

      } catch (PSQLException e) {
        // Unique violation => already stored (redelivery), safe to ACK
        if (isUniqueViolation(e)) {
          channel.basicAck(tag, false);
          System.out.println("Duplicate event_key=" + eventKey + " (already persisted). ACKed.");
        } else if (isTransientDbProblem(e)) {
          // transient DB issue => retry
          channel.basicNack(tag, false, true);
          System.err.println("Transient DB error. NACK requeue=true. event_key=" + eventKey + " err=" + e.getMessage());
        } else {
          // non-transient => DLQ if configured
          channel.basicNack(tag, false, false);
          System.err.println("Non-transient DB error. NACK requeue=false. event_key=" + eventKey + " err=" + e.getMessage());
        }

      } catch (Exception e) {
        // Processing error: decide retry/DLQ
        channel.basicNack(tag, false, false);
        System.err.println("Failed processing; NACK requeue=false. event_key=" + eventKey + " err=" + e.getMessage());
      }
    };

    CancelCallback onCancel = consumerTag ->
        System.err.println("Consumer cancelled: " + consumerTag);

    final String consumerTag = channel.basicConsume(queue, false, onMessage, onCancel);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        System.err.println("Shutdown requested. Cancelling consumer...");
        channel.basicCancel(consumerTag);
      } catch (Exception ignored) {}

      try { channel.close(); } catch (Exception ignored) {}
      try { rmqConn.close(); } catch (Exception ignored) {}
      try { dataSource.close(); } catch (Exception ignored) {}
    }));

    System.out.println("Consuming from queue: " + queue);
    Thread.currentThread().join();
  }

  // ----------------- DB -----------------

  private static void persistEvent(String eventKey, String msg) throws SQLException {
    try (java.sql.Connection dbConn = dataSource.getConnection()) {
      dbConn.setAutoCommit(false);
      try (java.sql.PreparedStatement ps = dbConn.prepareStatement(INSERT_SQL)) {
        ps.setString(1, eventKey);
        ps.setString(2, msg); 
        ps.executeUpdate();
      }
      dbConn.commit();
    }
  }

private static String extractTradeIdFromXml(String xml) throws Exception {
  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

  // Security hardening (important)
  dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
  dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
  dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
  dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
  dbf.setXIncludeAware(false);
  dbf.setExpandEntityReferences(false);

  DocumentBuilder builder = dbf.newDocumentBuilder();
  Document doc = builder.parse(new InputSource(new StringReader(xml)));

  XPath xpath = XPathFactory.newInstance().newXPath();

  // Adjust XPath if needed
  String tradeId = (String) xpath.evaluate(
      "//tradeId/text()",
      doc,
      XPathConstants.STRING
  );

  if (tradeId == null || tradeId.trim().isEmpty()) {
    throw new IllegalStateException("tradeId not found in XML");
  }

  return tradeId.trim();
}

  
  private static HikariDataSource buildDataSource() {
    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(mustEnv("PG_JDBC_URL"));
    cfg.setUsername(mustEnv("PG_USERNAME"));
    cfg.setPassword(mustEnv("PG_PASSWORD"));
    cfg.setMaximumPoolSize(Integer.parseInt(env("PG_POOL_SIZE", "10")));
    cfg.setConnectionTimeout(10_000);
    cfg.setIdleTimeout(60_000);
    cfg.setMaxLifetime(30 * 60_000);
    return new HikariDataSource(cfg);
  }

  // ----------------- Helpers -----------------

  private static boolean isUniqueViolation(PSQLException e) {
    return "23505".equals(e.getSQLState()); // unique_violation
  }

  private static boolean isTransientDbProblem(PSQLException e) {
    String state = e.getSQLState();
    if (state == null) return false;
    return state.startsWith("08") || state.startsWith("40") || "57P01".equals(state);
  }

  private static String headerValueToString(Object v) {
    if (v == null) return null;
    if (v instanceof byte[]) return new String((byte[]) v, StandardCharsets.UTF_8);
    return v.toString();
  }

  private static String headersToJson(Map<String, Object> headers) {
    if (headers == null || headers.isEmpty()) return "{}";

    Map<String, Object> safe = new HashMap<>();
    for (Map.Entry<String, Object> e : headers.entrySet()) {
      Object v = e.getValue();
      if (v instanceof byte[]) {
        safe.put(e.getKey(), Base64.getEncoder().encodeToString((byte[]) v));
      } else {
        safe.put(e.getKey(), v);
      }
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(safe);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize headers to JSON", e);
    }
  }

  // Java 8 replacement for String.isBlank()
  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }

  private static String mustEnv(String k) {
    String v = System.getenv(k);
    if (isBlank(v)) throw new IllegalStateException("Missing env var: " + k);
    return v;
  }

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return isBlank(v) ? def : v;
  }
}
