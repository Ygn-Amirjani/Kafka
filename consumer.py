from kafka import KafkaConsumer
from kafka import KafkaProducer
from queue import Queue

import datetime, time, json

# Load config file
with open('config.json', mode='r') as config_file:
    CONFIG = json.load(config_file)

Apache_kafka = f"{CONFIG.get('host')}:{CONFIG.get('port')}"
data = Queue()

def read_from_input_topic():
    consumer = KafkaConsumer(CONFIG.get('topics', {}).get('first_topic'),bootstrap_servers=Apache_kafka)
    for msg in consumer:
        rfc3339 = datetime.datetime.fromtimestamp(int(msg.value.decode())/1000).isoformat()
        data.put(rfc3339)
        print (msg)

# def send_message_to_output_topic():
#     producer = KafkaProducer(bootstrap_servers=Apache_kafka)
#     while True:
#         producer.send(CONFIG.get('topics', {}).get('second_topic'), json.dumps(data.get()).encode('utf-8'))
#         producer.flush()
#         time.sleep(5)

if __name__ == "__main__":
    read_from_input_topic()
    # send_message_to_output_topic()




