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

def log_to_database(connection, conf_id, status="info", message="", extract_date=date.today()):
    if not connection:
        logger.info(f"[LOG CONSOLE] conf_id={conf_id}, status={status}, message={message[:200]}")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO logs.extractLog (conf_id, status, message, created_at)
            VALUES (%s, %s, %s, %s)
        """, (conf_id, status, message[:500], extract_date))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error writing log DB: {e}")

# 2. Check extractProcess with conf_id, extract_date(default = today) and status = done, running
def check_process_running(connection, conf_id, extract_date):
    cursor = connection.cursor(buffered=True)
    cursor.execute("""
        SELECT 1 FROM process
        WHERE conf_id = %s AND extract_date = %s
          AND status IN ('running', 'done')
        LIMIT 1
    """, (conf_id, extract_date))
    exists = cursor.fetchone() is not None
    cursor.close()
    return exists


# 3. Insert a new extractProcess row with: conf_id, status = 'running', extract_date
def start_process(connection, conf_id, extract_date):
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO process (conf_id, start_time, status, extract_date)
        VALUES (%s, NOW(), 'running', %s)
    """, (conf_id, extract_date))
    connection.commit()
    process_id = cursor.lastrowid
    cursor.close()
    return process_id

def end_process(connection, process_id, status='done', remarks=None):
    cursor = connection.cursor()
    cursor.execute("""
        UPDATE process
        SET status = %s,
            end_time = NOW(),
            remarks = %s
        WHERE process_id = %s
    """, (status, remarks, process_id))
    connection.commit()
    cursor.close()

# 1. Load extract config from extractConf inputs: conf_id
def load_api_config_from_db(connection, conf_id):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM extractConf WHERE conf_id = %s", (conf_id,))
    conf = cursor.fetchone()
    cursor.close()
    if not conf:
        log_to_database(connection, conf_id, "error", f"Config id={conf_id} not found")
        return None
    log_to_database(connection, conf_id, "info", f"Loaded API config id={conf_id}: {conf['api_url']}")
    return conf

def call_rapidapi(api_conf, endpoint, params=None):
    url = api_conf["api_url"] + endpoint
    headers = {
        "x-rapidapi-key": api_conf["api_key"],
        "x-rapidapi-host": api_conf["api_host"]
    }
    try:
        response = requests.get(url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        return True, response.json()
    except Exception as e:
        return False, str(e)

def extract_hotels_reviews(api_conf, conf_id, connection, limit=50):
    checkin = (date.today() + timedelta(days=30)).isoformat()
    checkout = (date.today() + timedelta(days=31)).isoformat()
    params_hotels = {"id": api_conf["city_id"], "checkinDate": checkin, "checkoutDate": checkout}

    ok, hotels_response = call_rapidapi(api_conf, "/hotels/search-overnight", params_hotels)
    if not ok:
        log_to_database(connection, conf_id, "error", f"Failed to call hotel API: {hotels_response}")
        return False, []

    properties = hotels_response.get("data", {}).get("properties", [])
    if not properties:
        log_to_database(connection, conf_id, "error", "Hotel API returned empty list")
        return False, []

    hotel_list = []
    for p in properties[:5]:  # giới hạn demo
        property_id = p.get("propertyId")
        content = p.get("content", {})
        info = content.get("informationSummary", {})
        reviews_info = content.get("reviews", {})
        address_info = info.get("address", {})

        hotel_name = info.get("localeName", "N/A")
        country = address_info.get("country", {}).get("name", "")
        city = address_info.get("city", {}).get("name", "")
        area = address_info.get("area", {}).get("name", "")
        hotel_score = reviews_info.get("cumulative", {}).get("score", "")

        full_address = ", ".join(filter(None, [area, city, country]))

        hotel_data = {
            "propertyId": property_id,
            "name": hotel_name,
            "address": full_address,
            "hotel_score": hotel_score
        }

        params_reviews = {"propertyId": property_id, "limit": limit}
        ok_review, review_response = call_rapidapi(api_conf, "/hotels/reviews", params_reviews)
        if ok_review and review_response:
            hotel_data["reviews"] = review_response
            hotel_data["reviews_extracted_at"] = datetime.now().isoformat()
        else:
            hotel_data["reviews"] = {}
            log_to_database(connection, conf_id, "error", f"Failed to get reviews for hotel {property_id}")

        hotel_list.append(hotel_data)

    if not hotel_list:
        return False, []

    return True, hotel_list


def save_to_json(data, conf_id, connection, prefix="review"):
    today_str = datetime.now().strftime("%d-%m-%Y")
    folder = r"D:\Data_warehousing\extract"
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{prefix}_{today_str}.json")

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    log_to_database(connection, conf_id, "success", f"Saved file: {filename}")
    return filename


def run_extraction(config_file="config.xml", conf_id=1, extract_date=date.today()):
    config = load_config_from_xml(config_file)
    connection = connect_to_database(config)

    # 1. Load extract config from extractConf inputs: conf_id
    api_conf = load_api_config_from_db(connection, conf_id)
    if not api_conf:
        connection.close()
        return

    # 2. Check extractProcess with conf_id, extract_date(default = today) and status = done, running
    if check_process_running(connection, conf_id, extract_date):
        log_to_database(connection, conf_id, "info", "Job already running/done today, skipping.")
        connection.close()
        return

    # 3. Insert a new extractProcess row with: conf_id, status = 'running', message = 'Extraction started
    process_id = start_process(connection, conf_id, extract_date)
    log_to_database(connection, conf_id, "running", "Extraction started")

    try:
        # 4. Call Rapid API Agoda to extract hotels with reviews
        #    Input: api_conf (api_key, api_host, city_id...)
        ok, hotels_with_reviews = extract_hotels_reviews(api_conf, conf_id, connection)
        
        # 5b. download data into a .json file with name: review_dd/mm/yyyy.json in: D:/Data warehouse/extract/ folder
        if ok and hotels_with_reviews:
            
            file_name = save_to_json(hotels_with_reviews, conf_id, connection, "reviews")

            # 6b1. update extractProcess table with process_id status = 'done'
            end_process(connection, process_id, status='done', remarks=f"Saved JSON: {file_name}")
            # 6b2. insert new extractLog row table with status = 'succes', message = 'Extraction successfully completed'
            log_to_database(connection, conf_id, "success", "Extraction successfully completed")
        else:
            # 6a1. update extractProcess table with process_id, status = 'error'
            end_process(connection, process_id, status='error')
            # 6a2. insert new extractLog row table with status = 'error', message = 'failed to download'
            log_to_database(connection, conf_id, "error", "failed to download")

    except Exception as e:
        # 5a1. update extractProcess table with process_id, status = 'error'
        end_process(connection, process_id, status='error', remarks=str(e))
        # 5a2. insert new extractLog row table with status = 'error', message = 'failed to call API'
        log_to_database(connection, conf_id, "error", "failed to call API")

    finally:
        connection.close()



if __name__ == "__main__":
    run_extraction("config.xml", conf_id=1, extract_date=date.today())
