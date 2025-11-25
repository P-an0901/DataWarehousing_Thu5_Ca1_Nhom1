import requests
from datetime import datetime, date, timedelta
import json
import mysql.connector
import xml.etree.ElementTree as ET
import logging
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
def setup_console_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

logger = setup_console_logging()

def load_config_from_xml(config_file):
    tree = ET.parse(config_file)
    root = tree.getroot()
    config = {
        'host': root.find('.//database/host').text,
        'user': root.find('.//database/user').text,
        'password': root.find('.//database/password').text,
        'database': root.find('.//database/database').text,
        'port': int(root.find('.//database/port').text)
    }
    return config

def connect_to_database(config):
    conn = mysql.connector.connect(**config)
    logger.info("Connected to database.")
    return conn

# set process error nếu muốn chạy lại chương trình
def set_process(config_file="config.xml", conf_id = 1, extract_date=date.today(), remarks="set Error"):
    config = load_config_from_xml(config_file)
    connection = connect_to_database(config)
    cursor = connection.cursor()
    cursor.execute("""
        UPDATE extractProcess
        SET status = 'error', remarks = %s
        WHERE conf_id = %s AND extract_date = %s AND status = 'done'
    """, (remarks, conf_id, extract_date))
    connection.commit()
    cursor.close()


if __name__ == "__main__":
    set_process("config.xml", conf_id=1, extract_date=date.today())