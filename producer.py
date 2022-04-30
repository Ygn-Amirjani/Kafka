from kafka import KafkaProducer
import time, json

# Load config file
with open('config.json', mode='r') as config_file:
    CONFIG = json.load(config_file)

def send_message_to_input_topic():
    # It just needs to have at least one broker that will respond to a Metadata API Request. 
    Apache_kafka = f"{CONFIG.get('host')}:{CONFIG.get('port')}"
    producer = KafkaProducer(bootstrap_servers=Apache_kafka)

    while True: 
        """ continuously write messages to 'input' topic with epoch timestamp in ms """
        
        # epoch time
        current_time = int(time.time() * 1000)
        # Publish a message to a topic.
        msg = producer.send('input', json.dumps(current_time).encode('utf-8'))
        # block until all asynchronous messages are sent
        producer.flush()
        # We send a message every 5 seconds .
        time.sleep(5)

if __name__ == "__main__":
    send_message_to_input_topic()