import pytest
import paho.mqtt.client as mqtt
import mysql.connector
import json


@pytest.fixture(scope='function')
def mqtt_client():
    broker = "localhost"  # host name, replace with IP address
    client = mqtt.Client("mqtt_client")  # create client object
    client.connect(broker)  # establishing connection
    return client


@pytest.fixture(scope='function')
def db_connection():
    con = mysql.connector.connect(host='localhost',
                                  database='mqtt',
                                  user='root',
                                  password='123password',
                                  auth_plugin='mysql_native_password')
    return con


@pytest.fixture(scope='function')
def create_table_input():
    input_table = "CREATE TABLE IF NOT EXISTS input_mqtt (uuid VARCHAR(50), content VARCHAR(5000) NOT NULL)"
    return input_table


@pytest.fixture(scope='function')
def create_table_output():
    output_table = "CREATE TABLE IF NOT EXISTS input_mqtt (uuid VARCHAR(50), content VARCHAR(5000) NOT NULL)"
    return output_table


@pytest.fixture(scope='function')
def drop_table_input():
    drop_table = "DROP TABLE IF EXISTS input_mqtt"
    return drop_table


@pytest.fixture(scope='function')
def drop_table_output():
    drop_table = "DROP TABLE IF EXISTS output_mqtt"
    return drop_table


@pytest.fixture(scope='function')
def get_input_db(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("""SELECT content FROM input_data""")
    input_data = cursor.fetchall()
    json_list = [item for t in input_data for item in t]
    input_data = [json.loads(i) for i in json_list if i]
    return input_data


@pytest.fixture(scope='function')
def get_output_db(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("""SELECT content FROM output_data""")
    output_data = cursor.fetchall()
    json_list = [item for t in output_data for item in t]
    output_data = [json.loads(i) for i in json_list if i]
    return output_data
