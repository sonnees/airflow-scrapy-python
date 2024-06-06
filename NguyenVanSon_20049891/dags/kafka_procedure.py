import argparse
import csv
import json
import socket
from confluent_kafka import Producer

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print(f'Message produced: {msg}')

def send_message(bootstrap_servers, topic, csv_file):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': socket.gethostname(),
        'message.timeout.ms': 60000,
        'retries': 5,
        'retry.backoff.ms': 1000
    }

    producer = Producer(conf)
    # message_key = 'son'
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
            #   age,sex,chest_pain_type,resting_blood_pressure,cholesterol,fasting_blood_sugar,exercise_induced_angina,target
                message_value = json.dumps({
                    "age": row["age"],
                    "sex": row["sex"],
                    "chest_pain_type": row["chest_pain_type"],
                    "resting_blood_pressure": row["resting_blood_pressure"],
                    "cholesterol": row["cholesterol"],
                    "fasting_blood_sugar": row["fasting_blood_sugar"],
                    "exercise_induced_angina": row["exercise_induced_angina"],
                    "target": row["target"]
                }).encode('utf-8')

                # producer.produce(topic, key=message_key, value=message_value, callback=acked)
                producer.produce(topic, value=message_value, callback=acked)
                producer.poll(0)
    except BufferError as e:
        print(f"BufferError: {e}")  
    except Exception as e:
        print(f"Exception: {e}")
    
    producer.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Send messages to Kafka topic")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap server")
    parser.add_argument("--topic", required=True, help="Kafka topic")
    parser.add_argument("--csv-file", required=True, help="CSV file with data to send")
    args = parser.parse_args()
    
    send_message(args.bootstrap_servers, args.topic, args.csv_file)

