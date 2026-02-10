import com.rabbitmq.client.*;
import java.util.Map;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import org.postgresql.util.PGobject;

public class RabbitConsumer {

 private static HikariDataSource dataSource;

private static final String INSERT_SQL = loadSql("/sql/insert_incoming_event.sql");

private static String loadSql(String resourcePath) {
  try (InputStream is = RabbitConsumer.class.getResourceAsStream(resourcePath)) {
    if (is == null) {
      throw new IllegalStateException("SQL resource not found on classpath: " + resourcePath);
    }
    return new String(is.readAllBytes(), StandardCharsets.UTF_8);
  } catch (Exception e) {
    throw new RuntimeException("Failed to load SQL resource: " + resourcePath, e);
  }
}

  public static void main(String[] args) throws Exception {

    dataSource = buildDataSource();
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

    // Kubernetes / OpenShift friendly settings
    factory.setAutomaticRecoveryEnabled(true);
    factory.setTopologyRecoveryEnabled(true);
    factory.setNetworkRecoveryInterval(5000); // milliseconds (Java 8)
    factory.setRequestedHeartbeat(30);

    // TLS (AMQPS)
    factory.useSslProtocol();
    factory.enableHostnameVerification();

    Connection connection = factory.newConnection("rabbit-consumer");
    Channel channel = connection.createChannel();

    channel.basicQos(prefetch);

    boolean autoAck = false;

    // after creating channel:
    channel.queueDeclarePassive(queue);
    System.out.println("Queue exists: " + queue);
    
    DeliverCallback onMessage = (consumerTag, delivery) -> {
      long tag = delivery.getEnvelope().getDeliveryTag();
      System.out.println("Got delivery tag=" + tag +
          " redelivered=" + delivery.getEnvelope().isRedeliver() +
          " bytes=" + delivery.getBody().length);
    
      try {
        String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
        AMQP.BasicProperties props = delivery.getProperties();

  // Headers
  Map<String, Object> headers = props.getHeaders(); // can be null

  if (headers != null) {
    headers.forEach((k, v) -> {
      System.out.println("Header " + k + " = " + headerValueToString(v));
    });
  } else {
    System.out.println("No headers present");
  }

  // Other useful fields you might call “headers”
  System.out.println("contentType=" + props.getContentType());
  System.out.println("correlationId=" + props.getCorrelationId());
  System.out.println("messageId=" + props.getMessageId());
  System.out.println("timestamp=" + props.getTimestamp());
  System.out.println("type=" + props.getType());
  System.out.println("appId=" + props.getAppId());

  // Routing/queue metadata (not AMQP headers, but often useful)
  System.out.println("exchange=" + delivery.getEnvelope().getExchange());
  System.out.println("routingKey=" + delivery.getEnvelope().getRoutingKey());     


         String eventKey = props.getMessageId();
      if (eventKey == null) {
        Object tradeIdHeader = headers != null ? headers.get("tradeId") : null;
        if (tradeIdHeader != null) {
          eventKey = headerValueToString(tradeIdHeader);
        }
      }
      if (eventKey == null) {
        eventKey = "rk:" + delivery.getEnvelope().getRoutingKey() + "|tag:" + tag;
      }
        
        process(msg);
        persistEvent(eventKey, headers, msg);
        channel.basicAck(tag, false);
        System.out.println("Persisted + ACKed event_key=" + eventKey);
      } 
      catch (PSQLException e) {
        // Unique violation => already stored (redelivery), safe to ACK
        if (isUniqueViolation(e)) {
          channel.basicAck(tag, false);
          System.out.println("Duplicate event_key=" + eventKey + " (already persisted). ACKed.");
        } else if (isTransientDbProblem(e)) {
          // transient DB issue => requeue for retry
          channel.basicNack(tag, false, true);
          System.err.println("Transient DB error. NACK requeue=true. event_key=" + eventKey + " err=" + e.getMessage());
        } else {
          // non-transient DB error => don't loop forever; send to DLQ if configured
          channel.basicNack(tag, false, false);
          System.err.println("Non-transient DB error. NACK requeue=false. event_key=" + eventKey + " err=" + e.getMessage());
        }
      }
      catch (Exception e) {
        channel.basicNack(tag, false, false);
        System.err.println("Failed processing; nacked: " + e.getMessage());
      }
      
    };

    CancelCallback onCancel =
        consumerTag -> System.err.println("Consumer cancelled: " + consumerTag);

    String consumerTag = channel.basicConsume(queue, autoAck, onMessage, onCancel);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        System.err.println("Shutdown requested. Cancelling consumer...");
        channel.basicCancel(consumerTag);
      } catch (Exception ignored) {}

      try { channel.close(); } catch (Exception ignored) {}
      try { connection.close(); } catch (Exception ignored) {}
    }));

    System.out.println("Consuming from queue: " + queue);
    Thread.currentThread().join();
  }

  private static void process(String msg) {
    System.out.println("Received: " + msg);
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

private static String headerValueToString(Object v) {
  if (v == null) return "null";
  if (v instanceof byte[]) return new String((byte[]) v, StandardCharsets.UTF_8);
  return v.toString();
}

  // ----------------- DB -----------------

private static void persistEvent(String eventKey, Map<String, Object> headers, String msg) throws SQLException {
  // incoming_events(event_key TEXT UNIQUE, headers JSONB, payload_xml TEXT)

  PGobject jsonb = new PGobject();
  jsonb.setType("jsonb");
  jsonb.setValue(headersToJson(headers)); 

  try (Connection c = dataSource.getConnection()) {
    c.setAutoCommit(false);
    try (PreparedStatement ps = c.prepareStatement(INSERT_SQL)) {
      ps.setString(1, eventKey);
      ps.setObject(2, jsonb);
      ps.setString(3, msg);
      ps.executeUpdate();
    }
    c.commit();
  }
}



  private static HikariDataSource buildDataSource() {
    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(mustEnv("jdbc:postgresql://vsemxkph354706231380.ceas0akte0b1.eu-west-1.rds.amazonaws.com:5432/dbo"));          // jdbc:postgresql://host:5432/db
    cfg.setUsername(mustEnv("impownerbs1"));
    cfg.setPassword(mustEnv("hbSF0INu8:W7Vk4qQt1kz1LrNcjFuQ"));
    cfg.setMaximumPoolSize(Integer.parseInt(env("PG_POOL_SIZE", "10")));
    cfg.setConnectionTimeout(10_000);
    cfg.setIdleTimeout(60_000);
    cfg.setMaxLifetime(30 * 60_000);
    return new HikariDataSource(cfg);
  }

  // ----------------- Helpers -----------------

  private static boolean isUniqueViolation(PSQLException e) {
    // Postgres UNIQUE violation SQLSTATE = 23505
    return "23505".equals(e.getSQLState());
  }

  private static boolean isTransientDbProblem(PSQLException e) {
    // Common transient classes:
    // 08xxx = connection exceptions
    // 40xxx = transaction rollback (serialization/deadlock)
    String state = e.getSQLState();
    if (state == null) return false;
    return state.startsWith("08") || state.startsWith("40") || "57P01".equals(state); // admin shutdown
  }

  private static String headersToJson(Map<String, Object> headers) {
    if (headers == null || headers.isEmpty()) return "{}";

    StringBuilder sb = new StringBuilder("{");
    boolean first = true;

    for (var e : headers.entrySet()) {
      if (!first) sb.append(",");
      first = false;

      sb.append("\"").append(escapeJson(e.getKey())).append("\":");

      Object v = e.getValue();
      if (v == null) {
        sb.append("null");
      } else if (v instanceof Number || v instanceof Boolean) {
        sb.append(v);
      } else if (v instanceof byte[]) {
        sb.append("\"").append(escapeJson(new String((byte[]) v, StandardCharsets.UTF_8))).append("\"");
      } else {
        sb.append("\"").append(escapeJson(v.toString())).append("\"");
      }
    }

    sb.append("}");
    return sb.toString();
  }

  private static String escapeJson(String s) {
    return s
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t");
  }

 private static PGobject toJsonb(Map<String, Object> headers) throws SQLException {
  PGobject jsonb = new PGobject();
  jsonb.setType("jsonb");
  jsonb.setValue(toJson(headers)); // your JSON serialization (ideally Jackson)
  return jsonb;
}
  
  
}


