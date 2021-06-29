"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
        f"{constant.KAFKA_CONNECT_URI}/connectors",
        headers={"Content-Type": "application/json"},
        data=connector,
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")

if __name__ == "__main__":
    configure_connector()
