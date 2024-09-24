import pickle
import scipy.io
from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'your_group_id',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)

# Kafka topic
topic = 'traffic_data_v2'
consumer.subscribe([topic])

# Dictionary to store the received data
received_data = {}

# List of variables to receive
variables_to_receive = ['tra_X_te', 'tra_X_tr', 'tra_Y_te', 'tra_Y_tr', 'tra_adj_mat']

# Consume the messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        # Check if the key is not None
        if msg.key() is not None:
            var_name = msg.key().decode('utf-8')
            if var_name not in received_data:
                received_data[var_name] = bytearray()
            received_data[var_name].extend(msg.value())
            print(f"Received chunk for variable: {var_name}")
            if all(var in received_data for var in variables_to_receive):
                break
finally:
    consumer.close()

# Deserialize the received data
for var_name in received_data:
    received_data[var_name] = pickle.loads(received_data[var_name])

# Save the received data to a .mat file
output_mat_file_path = 'received_traffic_dataset.mat'
scipy.io.savemat(output_mat_file_path, received_data)

# Now you can access the received data
print(f"Data saved to {output_mat_file_path}")