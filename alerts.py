from confluent_kafka import Consumer
import json
from configs import kafka_config

# Налаштування для Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'alert_group',
    'auto.offset.reset': 'earliest'
}

# Створення споживача Kafka
consumer = Consumer({
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password'],
    'group.id': 'alert_group_nesvit',
    'auto.offset.reset': 'earliest'

})

# Підписка на топіки сповіщень
consumer.subscribe(['temperature_alerts_nesvit', 'humidity_alerts_nesvit'])

# Функція для зчитування сповіщень
def read_alerts():
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        alert = json.loads(msg.value().decode('utf-8'))
        print(f"Alert received: {alert}")

# Запуск зчитування сповіщень
read_alerts()
