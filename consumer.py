from confluent_kafka import Consumer, Producer
import json
from configs import kafka_config

# Створення споживача Kafka
consumer = Consumer({
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password'],
    'group.id': 'sensor_group_nesvit',
    'auto.offset.reset': 'earliest'
})

# Створення продюсера Kafka
producer = Producer({
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password']
})

# Підписка на топік
consumer.subscribe(['building_sensors_nesvit'])

# Функція для обробки даних
def process_data():
    while True:
        msg = consumer.poll(1.0)  # очікування повідомлення
        
        if msg is None:
            continue  # не було нових повідомлень
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        data = json.loads(msg.value().decode('utf-8'))
        sensor_id = data['sensor_id']
        temperature = data['temperature']
        humidity = data['humidity']
        timestamp = data['timestamp']
        
        # Перевірка температури
        if temperature > 40:
            alert = {
                'sensor_id': sensor_id,
                'timestamp': timestamp,
                'temperature': temperature,
                'message': 'Temperature exceeds threshold!'
            }
            producer.produce('temperature_alerts_nesvit', value=json.dumps(alert))
            producer.flush()

        # Перевірка вологості
        if humidity > 80 or humidity < 20:
            alert = {
                'sensor_id': sensor_id,
                'timestamp': timestamp,
                'humidity': humidity,
                'message': 'Humidity exceeds threshold!'
            }
            producer.produce('humidity_alerts_nesvit', value=json.dumps(alert))
            producer.flush()

# Запуск обробки даних
process_data()
