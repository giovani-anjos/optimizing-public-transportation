"""Configures KSQL to combine station and turnstile data"""
import configparser
import json
import logging
from pathlib import Path

import requests

import topic_check

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read("../config.ini")
KSQL_URL = config.get('env', 'ksql_server_uri')

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'org.chicago.cta.turnstile',
    VALUE_FORMAT = 'AVRO',
    KEY = 'station_id'
);
CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT = 'JSON') AS
    SELECT station_id, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("turnstile_summary") is True:
        logging.info("KSQL tables already exist")
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()