import time
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="logs", exchange_type="fanout", durable=True)


result = channel.queue_declare(queue="", exclusive=True, durable=True)
queue_name = result.method.queue
channel.queue_bind(exchange="logs", queue=queue_name)

print(" [*] Waiting for logs. To exit press Ctrl+C")


def callback(
    ch, method, properties, body
):  # pylint: disable=unused-argument, invalid-name
    time.sleep(2)
    print(f" [x] {body}")


channel.basic_qos(prefetch_count=1)


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)


channel.start_consuming()
