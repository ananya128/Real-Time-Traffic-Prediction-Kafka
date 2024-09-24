import pickle
import scipy.io
from confluent_kafka import Producer

# Load the .mat file
mat_file_path = 'traffic_dataset.mat'
mat_data = scipy.io.loadmat(mat_file_path)

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

# Kafka topic
topic = 'traffic_data_v2'

# List of variables to send
variables_to_send = ['tra_X_te', 'tra_X_tr', 'tra_Y_te', 'tra_Y_tr', 'tra_adj_mat']

# Function to split data into chunks
def split_data(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Produce messages for each variable
chunk_size = 100000  # 100 KB chunks
for var_name in variables_to_send:
    serialized_data = pickle.dumps(mat_data[var_name])
    for chunk in split_data(serialized_data, chunk_size):
        print(f"Sending chunk of size {len(chunk)} for variable {var_name}")
        producer.produce(topic, chunk, key=var_name.encode('utf-8'))

# Flush the producer
producer.flush()