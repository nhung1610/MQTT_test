import json
import time
import uuid

import pytest

import sys
from pathlib import Path

current_dir = Path(__file__).parent
_config_dir = None
# to ease test development the common dir is added to the PYTHONPATH
if str(current_dir.name) == "test":
    cp = current_dir / "common"
    cp = cp.resolve()
    sys.path.insert(0, str(cp))

from subscriber_fixture import *

parametrize = pytest.mark.parametrize


@parametrize(
    "topic, input_file_string",
    [
        ("test_topic1", "input/example_1.json"),
        ("test_topic2", "input/example_2.json"),
    ],
)
def test_publish(mqtt_client, topic, input_file_string):
    """
    Test publisher (read JSON input file/message and publish message to a topic)
    """
    # prepare
    input_file = Path(input_file_string)
    # act
    with open(input_file, 'r') as f:
        input_json = json.load(f)
    ret = mqtt_client.publish(topic, payload=json.dumps(input_json).encode("utf-8"))
    # assert
    assert ret.rc == 0


@parametrize(
    "broker, topic, input_file_string",
    [
        ("localhost", "test_topic1", "input/example_1.json"),
        ("localhost", "test_topic2", "input/example_2.json"),
    ],
)
def test_subscriber(mqtt_client, topic, broker, input_file_string):
    """
    Test subscriber (subscribed to a particular topic to receive the message in JSON)
    - Subscribe to a particular topic to receive the message in JSON
    - Validate input/expected output with actual output from MQTT
    (checks whether the contents of the two variables are identical)
    """
    # prepare
    input_file = Path(input_file_string)
    subscriber = Subscriber(broker, topic, mqtt_client)
    # act
    subscriber.start_receiving()
    # act - get input file
    with open(input_file, 'r', encoding='utf-8') as f:
        input_json = json.load(f)
    ret = mqtt_client.publish(topic, payload=json.dumps(input_json).encode("utf-8"))
    time.sleep(1)
    # act - output file = received
    received = subscriber.payload_json
    # assert
    assert ret.rc == 0
    assert sorted(input_json) == sorted(received)


def test_db_connection(db_connection):
    """
    Test database connection
    """
    # prepare
    cursor = db_connection.cursor()
    # act
    cursor.execute("SELECT VERSION()")
    results = cursor.fetchone()
    # assert
    assert results != ''


@parametrize(
    "input_file_string",
    [
        ("input/hotcakes.json"),
    ],
)
def test_insert_input_db(db_connection, drop_table_input, create_table_input, input_file_string):
    """
    Test if the input data table will be created and if the output message
    (which will run through MQTT) will be saved to the database
    """
    # prepare
    input_file = Path(input_file_string)
    input_file = open(input_file).read()
    cursor = db_connection.cursor()
    cursor.execute(drop_table_input)
    cursor.execute(create_table_input)
    # act - show table
    check_input_table = "SHOW TABLES LIKE 'input_mqtt'"
    cursor.execute(check_input_table)
    input_table_result = cursor.fetchone()
    # act - insert input data into db
    sql = """INSERT INTO input_mqtt (uuid, content) VALUES (%s, %s)"""
    val = (str(uuid.uuid4()), input_file)
    cursor.execute(sql, val)
    db_connection.commit()
    check_content = cursor.fetchone()
    # assert
    assert input_table_result != ''
    assert check_content != ''


@parametrize(
    "broker, topic, input_file_string",
    [
        ("localhost", "test_topic1", "input/example_1.json"),
    ],
)
def test_create_table_output(input_file_string, broker, topic, mqtt_client, db_connection, drop_table_output,
                             create_table_output):
    """
    Test if the output data table will be created and if the output message
    (which will run through MQTT) will be saved to the database
    """
    # prepare
    cursor = db_connection.cursor()
    cursor.execute(drop_table_output)
    cursor.execute(create_table_output)
    input_file = Path(input_file_string)
    subscriber = Subscriber(broker, topic, mqtt_client)
    # act - show table output_data
    check_output_table = "SHOW TABLES LIKE 'output_mqtt'"
    cursor.execute(check_output_table)
    output_table_result = cursor.fetchone()
    # act - publish and receive message
    subscriber.start_receiving()
    with open(input_file, 'r', encoding='utf-8') as f:
        input_json = json.load(f)
    ret = mqtt_client.publish(topic, payload=json.dumps(input_json).encode("utf-8"))
    time.sleep(1)
    received = subscriber.payload_json
    received = json.dumps(received)
    # act - load output data into db
    sql = """INSERT INTO output_mqtt (uuid, content) VALUES (%s, %s)"""
    val = (str(uuid.uuid4()), received)
    cursor.execute(sql, val)
    db_connection.commit()
    check_content = cursor.fetchone()
    # assert
    assert output_table_result != ''
    assert check_content != ''


def test_get_input_db(db_connection):
    """Test if the input message from database will be retrieved and saved in a variable"""
    # prepare
    cursor = db_connection.cursor()
    # act
    cursor.execute("""SELECT content FROM input_mqtt""")
    input_data = cursor.fetchall()
    input_data = [item for t in input_data for item in t]
    # assert
    assert input_data != ''


def test_get_output_db(db_connection):
    """
    Test if the output message from database will be retrieved and saved in a variable
    """
    # prepare
    cursor = db_connection.cursor()
    # act
    cursor.execute("""SELECT content FROM output_mqtt""")
    output_data = cursor.fetchall()
    output_data = [item for t in output_data for item in t]
    # assert
    assert output_data != ''


def test_compare_json_files(get_input_db, get_output_db):
    """
    Test if the input and output variables from the database are equal
    """
    # prepare
    input_json = get_input_db
    output_json = get_output_db
    # assert
    assert input_json == output_json


@parametrize(
    "topic",
    [
        ("test_topic1"),
    ],
)
def test_pub_from_db(get_output_db, mqtt_client, topic):
    """
    Test if the output data from the database will be published (MQTT)
    """
    # prepare
    output_json_db = get_output_db
    # act
    ret = mqtt_client.publish(topic, payload=json.dumps(output_json_db).encode("utf-8"))
    # assert
    assert ret.rc == 0



@parametrize(
    "broker, topic, input_file_string",
    [
        ("localhost", "test_topic1", "input/example_1.json"),
    ],
)
def test_sub_from_db(input_file_string, get_output_db, mqtt_client, topic, broker):
    """Test if the output data will be subscribed (MQTT) and
    check if the output data which will be received
    is identical with the original input file/expected output
    """
    # prepare
    input_file = Path(input_file_string)
    with open(input_file, 'r', encoding='utf-8') as f:
        input_json = json.load(f)
    output_json_db = get_output_db
    subscriber = Subscriber(broker, topic, mqtt_client)
    # act
    subscriber.start_receiving()
    ret = mqtt_client.publish(topic, payload=json.dumps(output_json_db).encode("utf-8"))
    time.sleep(1)
    received = subscriber.payload_json
    received = [item for t in received for item in t]
    # assert
    assert ret.rc == 0
    assert sorted(input_json) == sorted(received)

