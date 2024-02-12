import json
import random
import string
import sys

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer

# from caphca.etl.employee import Employee
# from caphca.etl.producer import employee_topic_name


class CaphcaConsumer:
    string_deserializer = StringDeserializer('utf_8')

    def __init__(self, host: str = "localhost", port: str = "29092",
                 group_id: str = ''.join(random.choice(string.ascii_lowercase) for i in range(10))):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(self.conf)
        self.keep_runnning = True

    def consume(self, topics, processing_func):
        try:
            self.consumer.subscribe(topics)
            while self.keep_runnning:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    print("Couldn't find a single message...")
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Error: {msg.error()}")
                    continue

                processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

def process_employee_record(msg_value):
    # Assuming msg_value is a dictionary with employee data and an 'action' key
    action = msg_value.get('action')
    emp_id = msg_value.get('emp_id')
    employee = (emp_id, msg_value.get('first_name'), msg_value.get('last_name'),
                msg_value.get('dob'), msg_value.get('city'))

    try:
        if action == 'INSERT':
            cur.execute(
                "INSERT INTO employees (emp_id, first_name, last_name, dob, city) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (emp_id) DO NOTHING",
                employee)
        elif action == 'UPDATE':
            cur.execute("UPDATE employees SET first_name=%s, last_name=%s, dob=%s, city=%s WHERE emp_id=%s",
                        employee[1:] + (emp_id,))
        elif action == 'DELETE':
            cur.execute("DELETE FROM employees WHERE emp_id=%s", (emp_id,))
    except Exception as e:
        print(f"Error processing record: {e}")

def persist_employee(msg):
    try:
        msg_value = json.loads(msg.value())

        conn = psycopg2.connect(
            host="localhost",
            database="postgres",  # Make sure this is the correct database name
            user="postgres",
            password="postgres",
            port="5433")  # Use the correct password
        conn.autocommit = True
        cur = conn.cursor()

        process_employee_record(msg_value, cur)

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to process message {msg}: {e}")


if __name__ == '__main__':
    employee_topic_name = "employee_data"
    consumer = CaphcaConsumer(group_id="employee_consumer_1")

    # Set up database connection


    try:
        consumer.consume([employee_topic_name], lambda msg: persist_employee(msg, cur))
    finally:
        print('Consumer loop has ended.')