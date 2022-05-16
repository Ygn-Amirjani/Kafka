from kafka import KafkaProducer
import time, json

def send_message_to_input_topic():
    """ continuously write messages to 'input' topic with epoch timestamp in ms. """

    # It just needs to have at least one broker that will respond to a Metadata API Request. 
    producer = KafkaProducer(bootstrap_servers='kafka-exporter.apache-kafka.svc.cluster.local:9092')

    while True: 
        current_time = int(time.time() * 1000)
        # Publish a message to a topic.
        producer.send('input', json.dumps(current_time).encode('utf-8'))
        # block until all asynchronous messages are sent
        producer.flush()
        # We send a message every 5 seconds .
        time.sleep(1)

if __name__ == "__main__":
    send_message_to_input_topic()
