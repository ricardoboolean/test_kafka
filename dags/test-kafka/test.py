# kafka_example_dag_1.py 

import os
import json
import logging
import functools
from pendulum import datetime

from airflow import DAG
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

# get the topic name from .env
my_topic = os.environ["KAFKA_TOPIC_NAME"]

# get Kafka configuration information
connection_config = {
    "bootstrap.servers": os.environ["BOOSTRAP_SERVER"],
    "security.protocol": os.environ["SECURITY_PROTOCOL"],
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"]
}

with DAG(
    dag_id="kafka_example_dag_1",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
):

    # define the producer function
    def producer_function():
        for i in range(5):
            yield (json.dumps(i), json.dumps(i+1))

    # define the producer task
    producer_task = ProduceToTopicOperator(
        task_id=f"produce_to_{my_topic}",
        topic=my_topic,
        producer_function=producer_function, 
        kafka_config=connection_config
    )
