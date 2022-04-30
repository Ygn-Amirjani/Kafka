from kafka import KafkaProducer
import time, json

# It just needs to have at least one broker that will respond to a Metadata API Request. 
producer = KafkaProducer(bootstrap_servers='localhost:9092')

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