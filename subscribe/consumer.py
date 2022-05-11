from kafka import KafkaConsumer
from kafka import KafkaProducer
import datetime, json

# Load config file
with open('../config.json', mode='r') as config_file:
    CONFIG = json.load(config_file)

def send_message_to_output_topic():
    """ reads from 'input' topic, transforms input message to date string 
        (must be in RFC 3339) and sends to topic 'output'"""

    # It just needs to have at least one broker that will respond to a Metadata API Request. 
    Apache_kafka = f"{CONFIG.get('host')}:{CONFIG.get('port')}"
    consumer = KafkaConsumer(CONFIG.get('topics', {}).get('first_topic'),bootstrap_servers=Apache_kafka)
    producer = KafkaProducer(bootstrap_servers=Apache_kafka)

    for msg in consumer:
        rfc3339 = datetime.datetime.fromtimestamp(int(msg.value.decode())/1000).isoformat()
        # Publish a message to a topic.
        producer.send(CONFIG.get('topics', {}).get('second_topic'), json.dumps(rfc3339).encode('utf-8'))
        # block until all asynchronous messages are sent
        producer.flush()
        print (msg)

if __name__ == "__main__":
    send_message_to_output_topic()
