from datetime import datetime
import sys
import pika


connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

now = datetime.now()
date_time = now.strftime("%d.%m.%Y, %H:%M:%S")

message = " ".join(sys.argv[1:]) or f'info: Current time: {(datetime.now().strftime("%d.%m.%Y, %H:%M:%S"))}'


channel.exchange_declare(exchange="logs", exchange_type="fanout", durable=True)

channel.basic_publish(
    exchange="logs",
    routing_key="",
    body=message,
    properties=pika.BasicProperties(delivery_mode=2),
)
print(f" [x] Sent {message}")


channel.close()

connection.close()
