import os
import pika
url = os.getenv("RABBITMQ_URL")
if not url:
   host = os.getenv("RABBITMQ_HOST", "rabbitmq")
   port = int(os.getenv("RABBITMQ_PORT", "5672"))
   user = os.getenv("RABBITMQ_USERNAME", "guest")
   pw   = os.getenv("RABBITMQ_PASSWORD", "guest")
   vhost = os.getenv("RABBITMQ_VHOST", "/")
   url = f"amqp://{user}:{pw}@{host}:{port}{vhost}"
params = pika.URLParameters(url)
conn = pika.BlockingConnection(params)
ch = conn.channel()
q = os.getenv("QUEUE_NAME", "hello")
ch.queue_declare(queue=q, durable=True)
ch.basic_publish(exchange="", routing_key=q, body=b"hello from openshift")
print("sent")
conn.close()
