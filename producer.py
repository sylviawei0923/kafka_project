import json
import sys
print(sys.version)
from kafka import KafkaProducer
import psycopg2
#
employee_topic_name = "employee_data_8thDec"


class CaphcaProducer:
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        self.producer = KafkaProducer(bootstrap_servers=('%s:%s' % (self.host, self.port)),
                                      value_serializer=lambda m: json.dumps(m).encode('ascii'))

    def producer_msg(self, topic_name, key, value):
        self.producer.send(topic=topic_name, key=key, value=value)
        self.producer.flush()


class PostgresConnector:
    def __init__(self, dbname="postgres", user="postgres", password="postgres", host="localhost", port="5434"):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    def fetch_cdc_data(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT cdc_id, emp_id, first_name, last_name, dob, city, action FROM employees_cdc ORDER BY cdc_id ASC;")
            records = cur.fetchall()
            # Assuming you clear the CDC table after reading; otherwise, you need a mechanism to track processed records
            cur.execute("TRUNCATE employees_cdc;")
            self.conn.commit()
            return records


if __name__ == '__main__':
    postgres = PostgresConnector()
    producer = CaphcaProducer()

    cdc_data = postgres.fetch_cdc_data()
    for record in cdc_data:
        cdc_id, emp_id, first_name, last_name, dob, city, action = record
        change_data = {
            "cdc_id": cdc_id,
            "emp_id": emp_id,
            "first_name": first_name,
            "last_name": last_name,
            "dob": str(dob),  # Converting date to string for JSON serialization
            "city": city,
            "action": action
        }
        producer.producer_msg(employee_topic_name, key=str(emp_id).encode(), value=change_data)
