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
