import com.rabbitmq.client.*;
import java.util.Map;

import java.nio.charset.StandardCharsets;

public class RabbitConsumer {

  public static void main(String[] args) throws Exception {
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
        
        process(msg);
        channel.basicAck(tag, false);
      } catch (Exception e) {
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
}

private static String headerValueToString(Object v) {
  if (v == null) return "null";
  if (v instanceof byte[]) return new String((byte[]) v, StandardCharsets.UTF_8);
  return v.toString();
}
