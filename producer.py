from kafka import KafkaProducer
import time, json

# Load config file
with open('config.json', mode='r') as config_file:
    CONFIG = json.load(config_file)

def send_message_to_input_topic():
    """ continuously write messages to 'input' topic with epoch timestamp in ms. """

    # It just needs to have at least one broker that will respond to a Metadata API Request. 
    Apache_kafka = f"{CONFIG.get('host')}:{CONFIG.get('port')}"
    producer = KafkaProducer(bootstrap_servers=Apache_kafka)

    while True: 
        current_time = int(time.time() * 1000)
        # Publish a message to a topic.
        producer.send(CONFIG.get('topics', {}).get('first_topic'), json.dumps(current_time).encode('utf-8'))
        # block until all asynchronous messages are sent
        producer.flush()
        # We send a message every 5 seconds .
        time.sleep(60)

if __name__ == "__main__":
    send_message_to_input_topic()
