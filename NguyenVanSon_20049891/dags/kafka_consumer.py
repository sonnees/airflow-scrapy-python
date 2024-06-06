import argparse
import json
import os
from confluent_kafka import Consumer, KafkaException
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from pandas_gbq import gbq
import google.oauth2.service_account as service_account

def kafka_bigquery(bootstrap_servers, topic, consumer_group):
    def get_message(bootstrap_server, topic, consumer_group):
        conf = {
            'bootstrap.servers': bootstrap_server,
            'group.id': consumer_group,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(conf)
        consumer_loop(consumer, topic)

    def consumer_loop(consumer, topic):
        credentials = service_account.Credentials.from_service_account_file('./son.json')
        project_id = 'stellar-works-416715'
        table_id = 'stellar-works-416715.son.son1'
        consumer.subscribe([topic])
        #   User Id,First Name,Sex,Email,Phone,Date of birth
        try:
            column=['age', 'sex','chest_pain_type', 'resting_blood_pressure', 'cholesterol', 'fasting_blood_sugar', 'exercise_induced_angina', 'target']
            df = pd.DataFrame(columns = column)
            df = df.astype({'age': str, 'sex': str, 'chest_pain_type': str, 'resting_blood_pressure': str, 'cholesterol': str, 'fasting_blood_sugar': str, 'exercise_induced_angina': str, 'target': str})

            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    print('Waiting for message or event/error in poll()', msg)
                    if (df.shape[0] > 0):
                        print("df", df)
                        gbq.to_gbq(df,destination_table= table_id, project_id=project_id, credentials = credentials)  
                        print("Order successfully sent to table in BigQuery!")
                    continue
                if msg.error():
                    print(f'Error: {msg.error()}')
                else:
                    message_data = json.loads(msg.value().decode('utf-8'))  
            #    age,sex,chest_pain_type,resting_blood_pressure,cholesterol,fasting_blood_sugar,exercise_induced_angina,target

                    df.loc[len(df.index)] = [
                        message_data['age'],
                        message_data['sex'],
                        message_data['chest_pain_type'],
                        message_data['resting_blood_pressure'],
                        message_data['cholesterol'],
                        message_data['fasting_blood_sugar'],
                        message_data['exercise_induced_angina'],
                        message_data['target']
                    ]
                    
        except KafkaException as e:
            print(f'KafkaException: {e}')
        finally:
            consumer.close()
    
        

    get_message(bootstrap_servers, topic, consumer_group)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consume messages from Kafka topic and send to BigQuery")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap server")
    parser.add_argument("--topic", required=True, help="Kafka topic")
    parser.add_argument("--consumer-group", required=True, help="Kafka consumer group")
    args = parser.parse_args()
    
    kafka_bigquery(args.bootstrap_servers, args.topic, args.consumer_group)