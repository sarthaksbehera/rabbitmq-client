import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class RabbitConsumer {

  public static void main(String[] args) throws Exception {
    String host = mustEnv("RABBIT_HOST");
    int port = Integer.parseInt(env("RABBIT_PORT", "5671"));
    String user = mustEnv("RABBIT_USERNAME");
    String pass = mustEnv("RABBIT_PASSWORD");
    String vhost = env("RABBIT_VHOST", "e-star-trades");
    String queue = env("RABBIT_QUEUE", "INTERNALMARKET.TRADES.INTERNAL");
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
    factory.setNetworkRecoveryInterval(5000); // MUST be int for Java 8
    factory.setRequestedHeartbeat(30);

    // TLS (only if using AMQPS)
    factory.useSslProtocol();
    factory.enableHostnameVerification();

    Connection connection = factory.newConnection("rabbit-consumer");
    Channel channel = connection.createChannel();

    // Limit in-flight messages per consumer
    channel.basicQos(prefetch);

    boolean autoAck = false;

    DeliverCallback onMessage = (consumerTag, delivery) -> {
      long tag = delivery.getEnvelope().getDeliveryTag();
      try {
        String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);

        // ---- your processing here ----
        process(msg);
        // ------------------------------

        channel.basicAck(tag, false);
      } catch (Exception e) {
        // Avoid poison-message loops: requeue=false (use DLQ on broker side)
        channel.basicNack(tag, false, false);
        // Optional: log
        System.err.println("Failed processing message; nacked (requeue=false). " + e.getMessage());
      }
    };

    CancelCallback onCancel = consumerTag -> System.err.println("Consumer cancelled: " + consumerTag);

    String consumerTag = channel.basicConsume(queue, autoAck, onMessage, onCancel);

    // Graceful shutdown: stop consuming, close channel/connection
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        System.err.println("Shutdown requested. Cancelling consumer...");
        channel.basicCancel(consumerTag);
      } catch (Exception ignored) {}

      try { channel.close(); } catch (Exception ignored) {}
      try { connection.close(); } catch (Exception ignored) {}
    }));

    System.out.println("Consuming from queue: " + queue);
    // Keep process alive (main thread can just block)
    Thread.currentThread().join();
  }

  private static void process(String msg) {
    // TODO real work
    System.out.println("Received: " + msg);
  }

  private static String mustEnv(String k) {
    String v = System.getenv(k);
    if (v == null || v.isBlank()) throw new IllegalStateException("Missing env var: " + k);
    return v;
  }

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return (v == null || v.isBlank()) ? def : v;
  }
}
