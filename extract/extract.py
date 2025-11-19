import requests
from datetime import datetime, date, timedelta
import json
import mysql.connector
import xml.etree.ElementTree as ET
import logging
import os

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
    logger.info("Kết nối DB thành công.")
    return conn

def log_to_database(connection, conf_id, status="info", message="", extract_date=date.today()):
    if not connection:
        logger.info(f"[LOG CONSOLE] conf_id={conf_id}, status={status}, message={message[:200]}")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO extractLog (conf_id, status, message, created_at)
            VALUES (%s, %s, %s, %s)
        """, (conf_id, status, message[:500], extract_date))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Lỗi ghi log DB: {e}")

def load_api_config_from_db(connection, conf_id):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM extractConf WHERE conf_id = %s", (conf_id,))
    conf = cursor.fetchone()
    cursor.close()

    if not conf:
        logger.error(f"Không tìm thấy config id={conf_id}")
        log_to_database(connection, conf_id, "error", f"Không tìm thấy config id={conf_id}")
        return None

    log_to_database(connection, conf_id, "info", f"Đã load config API id={conf['conf_id']}: {conf['api_url']}")
    return conf

def already_running_or_done(connection, conf_id, extract_date):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM extractLog
        WHERE conf_id = %s
          AND DATE(created_at) = %s
          AND status IN ('done')
    """, (conf_id, extract_date))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

def extract_hotels_list(api_conf, conf_id, connection):
    if not api_conf:
        log_to_database(connection, conf_id, "warning", "Không có config API để chạy.")
        return []

    log_to_database(connection, conf_id, "info", "Đang gọi API lấy danh sách khách sạn...")
    headers = {
        "x-rapidapi-key": api_conf['api_key'],
        "x-rapidapi-host": api_conf['api_host']
    }
    checkin = (date.today() + timedelta(days=30)).isoformat()
    checkout = (date.today() + timedelta(days=31)).isoformat()
    params = {"id": api_conf['city_id'], "checkinDate": checkin, "checkoutDate": checkout}

    try:
        response = requests.get(api_conf['api_url'] + '/hotels/search-overnight', headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        hotels = data.get("data", {}).get("citySearch", {}).get("properties", [])
        return hotels
    
    #4a. insert new extractLog row table with status = 'error', message = 'failed to call API'
    except Exception as e:
        log_to_database(connection, conf_id, "error", f"failed to call API")
        return []

def extract_hotel_reviews(hotel_list, api_conf, conf_id, connection, limit=50):
    if not hotel_list:
        return []

    log_to_database(connection, conf_id, 'info', 'Bắt đầu lấy review khách sạn')
    url_reviews = f"https://{api_conf['api_host']}/hotels/reviews"
    headers = {
        "x-rapidapi-key": api_conf['api_key'],
        "x-rapidapi-host": api_conf['api_host']
    }

    for hotel in hotel_list[:5]:
        property_id = hotel.get("propertyId") or hotel.get("id")
        querystring = {"propertyId": property_id, "limit": limit}
        try:
            response = requests.get(url_reviews, headers=headers, params=querystring, timeout=15)
            response.raise_for_status()
            hotel["reviews"] = response.json()
            hotel["reviews_extracted_at"] = datetime.now().isoformat()
            log_to_database(connection, conf_id, 'success', f'Đã lấy review cho: {hotel.get("name","N/A")}')
        except Exception as e:
            #4a. insert new extractLog row table with status = 'error', message = 'failed to call API'
            hotel["reviews"] = {"error": str(e)}
            log_to_database(connection, conf_id, 'error', f'failed to call API')

    log_to_database(connection, conf_id, 'info', f'Hoàn tất lấy review cho {len(hotel_list)} khách sạn')
    return hotel_list

def save_to_json(data, conf_id, connection, prefix="review"):
    today_str = datetime.now().strftime("%d-%m-%Y")
    folder = r"D:\Data warehousing\extract"
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{prefix}_{today_str}.json")

    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        log_to_database(connection, conf_id, "success", f"Đã lưu file: {filename}")
        return filename

    except Exception as e:
    #6a. insert new extractLog row table with status = 'error', message = 'failed to download'
        log_to_database(connection, conf_id, "error", "failed to download")
        return None


def run_extraction(config_file="config.xml", conf_id=1, extract_date=date.today()):
    #0. Connecting Conf database input: config.xml
    config = load_config_from_xml(config_file)
    connection = connect_to_database(config)
    #1.Load extractConf inputs: conf_id, extract_date(default = today)
    api_conf = load_api_config_from_db(connection, conf_id)
    # if not api_conf:
    #     log_to_database(connection, conf_id, "warning", "Không có config API hợp lệ để chạy.")
    #     return

    #2.1 check extractLog with extract_date
    if already_running_or_done(connection, conf_id, extract_date):
        log_to_database(connection, conf_id, "info", f"Config id={conf_id} đã chạy hôm nay, dừng extraction.")
        return

    #3. set status = 'running' in extractLog
    log_to_database(connection, conf_id, "running", "extracting")
    
    #4.Call Rapid API with url
    #5. Extract all review info based on input date
    hotels = extract_hotels_list(api_conf, conf_id, connection)
    hotels_with_reviews = extract_hotel_reviews(hotels, api_conf, conf_id, connection)

    #6. download data into a .json file with name: review_dd/mm/yyyy.json in: D:/Data warehouse/extract/
    file_name = save_to_json(hotels_with_reviews, conf_id, connection, "reviews")
    
    #7. set extractLog  status = 'done', message = 'extract successfully'
    log_to_database(connection, conf_id, "done", f"extract successfully")
    connection.close()

if __name__ == "__main__":
    run_extraction("config.xml", conf_id=1, extract_date=date.today())
