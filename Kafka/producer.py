import sys
from datetime import datetime
from kafka import KafkaProducer

HOST='localhost:9092'
TOPIC='my_topic'
k_data = KafkaProducer(bootstrap_servers=HOST)
producer = KafkaProducer(
    bootstrap_servers=[HOST],
    api_version=k_data.config['api_version'],
)
i = 0

message = ' '.join(sys.argv[1:]) or f'info: Current time: {(datetime.now().strftime("%d.%m.%Y, %H:%M:%S"))}'
producer.send(
    TOPIC,
    f'main {message}'.encode('utf-8'),
)

print(f' [x] Sent main {message}')
producer.send(
    TOPIC,
    f'retry {message}'.encode('utf-8'),
)
print(f' [x] Sent retry {message}')

producer.send(
    TOPIC,
    f'error {message}'.encode('utf-8'),
)
print(f' [x] Sent error {message}')
producer.flush()
