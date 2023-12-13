import os
from kafka import KafkaConsumer, KafkaProducer

HOST='localhost:9092'
k_data = KafkaProducer(bootstrap_servers=HOST)


consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers=[HOST],
    api_version=k_data.config['api_version'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.environ.get('CONSUMER_GROUP', 'default-group'),
)

producer = KafkaProducer(
    bootstrap_servers=[HOST],
    api_version=k_data.config['api_version'],
)

for msg in consumer:
    message = msg.value.decode('utf-8')
    print(f'[received] {message}')
    if 'main' in message:
        pass
    elif 'retry' in message:
        producer.send(
            'retry-topic',
            message.encode('utf-8'),
        )
        print(f'[Retry] {message}')
    elif 'error' in message:
        producer.send(
            'dead-letter-topic',
            message.encode('utf-8'),
        )
        print(f'[DLT] {message}')
