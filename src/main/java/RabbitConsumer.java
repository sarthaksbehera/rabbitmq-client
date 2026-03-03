import com.rabbitmq.client.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.postgresql.util.PSQLException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import java.io.StringReader;

// -------- Qpid JMS (AMQP 1.0) for Red Hat AMQ 7.13 --------
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public class RabbitConsumer {

  // -------------------- DB --------------------
  private static HikariDataSource dataSource;
  private static final String INSERT_SQL = loadSql("/sql/insert_incoming_event.sql");

  // -------------------- AMQ (Qpid JMS) --------------------
  private static Connection amqConn;
  private static Session amqSession;
  private static MessageProducer amqProducer;
  private static Destination amqDestination;

  // Namespace-safe XPath
  private static final String TRADE_ID_XPATH = "//*[local-name()='tradeId']/text()";

  // -------------------- Main --------------------
  public static void main(String[] args) throws Exception {

    dataSource = buildDataSource();
    initAmqProducerTopic(); // AMQPS topic producer

    // ---- RabbitMQ connection (AMQPS/TLS) ----
    String host = mustEnv("RABBIT_HOST");
    int port = Integer.parseInt(env("RABBIT_PORT", "5671"));
    String user = mustEnv("RABBIT_USERNAME");
    String pass = mustEnv("RABBIT_PASSWORD");
    String vhost = env("RABBIT_VHOST", "/");
    String queue = mustEnv("RABBIT_QUEUE");
    int prefetch = Integer.parseInt(env("RABBIT_PREFETCH", "50"));

    com.rabbitmq.client.ConnectionFactory rabbitFactory = new com.rabbitmq.client.ConnectionFactory();
    rabbitFactory.setHost(host);
    rabbitFactory.setPort(port);
    rabbitFactory.setUsername(user);
    rabbitFactory.setPassword(pass);
    rabbitFactory.setVirtualHost(vhost);

    rabbitFactory.setAutomaticRecoveryEnabled(true);
    rabbitFactory.setTopologyRecoveryEnabled(true);
    rabbitFactory.setNetworkRecoveryInterval(5000);
    rabbitFactory.setRequestedHeartbeat(30);

    rabbitFactory.useSslProtocol();
    rabbitFactory.enableHostnameVerification();

    final com.rabbitmq.client.Connection rmqConn = rabbitFactory.newConnection("rabbit-consumer");
    final Channel channel = rmqConn.createChannel();

    channel.basicQos(prefetch);
    channel.queueDeclarePassive(queue);
    System.out.println("Connected to RabbitMQ. Queue exists: " + queue);

    DeliverCallback onMessage = (consumerTag, delivery) -> {
      long tag = delivery.getEnvelope().getDeliveryTag();

      String receivedAt = ZonedDateTime.now()
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

      System.out.println("[" + receivedAt + "] Message received from RabbitMQ tag=" + tag);

      String eventKey = tag;

      try {
        String xml = new String(delivery.getBody(), StandardCharsets.UTF_8);

        // tradeId from XML becomes our idempotency key

        // 1) Persist to Postgres
        persistEvent(eventKey, xml, queue);

        // 2) Publish to AMQ Topic over AMQPS
        publishToAmqTopic(eventKey, xml);

        // 3) ACK Rabbit only after DB + AMQ succeed
        channel.basicAck(tag, false);

        System.out.println("Persisted + published to AMQ topic + ACKed eventKey=" + eventKey);

      } catch (PSQLException e) {
        if (isUniqueViolation(e)) {
          // Already in DB. Still publish to AMQ; if publish succeeds, ACK.
          try {
            String xml = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (eventKey == null) eventKey = extractFromXml(xml, TRADE_ID_XPATH);

            publishToAmqTopic(eventKey, xml);
            channel.basicAck(tag, false);

            System.out.println("Duplicate DB row; published to AMQ topic + ACKed eventKey=" + eventKey);
          } catch (Exception ex) {
            channel.basicNack(tag, false, true);
            System.err.println("DB duplicate but AMQ publish failed; NACK requeue=true eventKey=" + eventKey +
                " err=" + ex.getMessage());
          }
        } else if (isTransientDbProblem(e)) {
          channel.basicNack(tag, false, true);
          System.err.println("Transient DB error; NACK requeue=true eventKey=" + eventKey + " err=" + e.getMessage());
        } else {
          channel.basicNack(tag, false, false);
          System.err.println("Non-transient DB error; NACK requeue=false eventKey=" + eventKey + " err=" + e.getMessage());
        }

      } catch (Exception e) {
        // If AMQ is temporarily unavailable you might want requeue=true
        channel.basicNack(tag, false, true);
        System.err.println("Processing failed; NACK requeue=true eventKey=" + eventKey + " err=" + e.getMessage());
      }
    };

    CancelCallback onCancel = consumerTag ->
        System.err.println("Consumer cancelled: " + consumerTag);

    final String consumerTag = channel.basicConsume(queue, false, onMessage, onCancel);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try { channel.basicCancel(consumerTag); } catch (Exception ignored) {}
      try { channel.close(); } catch (Exception ignored) {}
      try { rmqConn.close(); } catch (Exception ignored) {}
      try { closeAmqQuietly(); } catch (Exception ignored) {}
      try { dataSource.close(); } catch (Exception ignored) {}
    }));

    System.out.println("Consuming from Rabbit queue: " + queue);
    Thread.currentThread().join();
  }

  // -------------------- AMQ Topic (Qpid JMS) --------------------

  private static void initAmqProducerTopic() throws Exception {
    String url = mustEnv("AMQ_AMQPS_URL");     // e.g. amqps://amq-broker:5671?transport.verifyHost=true
    String user = mustEnv("AMQ_USERNAME");
    String pass = mustEnv("AMQ_PASSWORD");
    String topicName = mustEnv("AMQ_TOPIC");

    JmsConnectionFactory cf = new JmsConnectionFactory(user, pass, url);

    amqConn = cf.createConnection();
    amqConn.start();

    amqSession = amqConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // Topic destination
    amqDestination = amqSession.createTopic(topicName);

    amqProducer = amqSession.createProducer(amqDestination);
    amqProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

    System.out.println("Connected to AMQ over AMQPS. Publishing to topic: " + topicName);
  }

  private static void publishToAmqTopic(String tradeId, String xml) throws Exception {
    TextMessage out = amqSession.createTextMessage(xml);
    out.setStringProperty("tradeId", tradeId);
    out.setJMSCorrelationID(tradeId);
    amqProducer.send(out);
  }

  private static void closeAmqQuietly() {
    try { if (amqProducer != null) amqProducer.close(); } catch (Exception ignored) {}
    try { if (amqSession != null) amqSession.close(); } catch (Exception ignored) {}
    try { if (amqConn != null) amqConn.close(); } catch (Exception ignored) {}
  }

  // -------------------- DB --------------------

  private static void persistEvent(String eventKey, String xml, String queue) throws SQLException {
    try (java.sql.Connection dbConn = dataSource.getConnection()) {
      dbConn.setAutoCommit(false);
      try (java.sql.PreparedStatement ps = dbConn.prepareStatement(INSERT_SQL)) {
        ps.setString(1, eventKey);
        ps.setString(2, xml);
        ps.setString(3, queue);
        ps.executeUpdate();
      }
      dbConn.commit();
    }
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

  // -------------------- XML helpers --------------------

  private static String extractFromXml(String xml, String xpathExpr) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

    dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
    dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    dbf.setXIncludeAware(false);
    dbf.setExpandEntityReferences(false);

    DocumentBuilder builder = dbf.newDocumentBuilder();
    Document doc = builder.parse(new InputSource(new StringReader(xml)));

    XPath xpath = XPathFactory.newInstance().newXPath();
    String value = (String) xpath.evaluate(xpathExpr, doc, XPathConstants.STRING);

    if (value == null || value.trim().isEmpty()) {
      throw new IllegalStateException("Required XML field not found for XPath: " + xpathExpr);
    }
    return value.trim();
  }

  // -------------------- SQL loader (Java 8) --------------------

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

  // -------------------- PG error helpers --------------------

  private static boolean isUniqueViolation(PSQLException e) {
    return "23505".equals(e.getSQLState());
  }

  private static boolean isTransientDbProblem(PSQLException e) {
    String state = e.getSQLState();
    return state != null && (state.startsWith("08") || state.startsWith("40") || "57P01".equals(state));
  }

  // -------------------- Env helpers --------------------

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
