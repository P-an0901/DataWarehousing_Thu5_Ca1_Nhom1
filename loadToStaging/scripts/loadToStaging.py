import os
import json
import mysql.connector
from datetime import datetime, timedelta

# -----------------------------
# 1. CONNECT DATABASE
# -----------------------------
def get_connection():
    return mysql.connector.connect(
        host="192.168.2.7",
        user="staging",
        password="123",
        database="Staging"
    )

# -----------------------------
# 2. GHI LOG loadToStagingLog
# -----------------------------
def insert_log_start(cursor, config_id, file_name):
    start_time = datetime.now()
    query = """
        INSERT INTO logs.loadToStagingLog (config_id, start_time, status, file_processed, created_at)
        VALUES (%s, %s, 'RUNNING', %s, CURDATE())
    """
    cursor.execute(query, (config_id, start_time, file_name))
    return cursor.lastrowid, start_time


def update_log_end(cursor, log_id, total_records, status, message):
    end_time = datetime.now()
    query = """
        UPDATE logs.loadToStagingLog
        SET end_time = %s, total_records = %s, status = %s, message = %s
        WHERE log_id = %s
    """
    cursor.execute(query, (end_time, total_records, status, message, log_id))


# -----------------------------
# 3. ĐỌC FILE JSON
# -----------------------------
def load_json_data(file_path):
    with open(file_path, "r", encoding="utf8") as f:
        return json.load(f)

# -----------------------------
# 3.5 INSERT NEW CONFIG cho ngày tiếp theo
# -----------------------------
def insert_next_day_config(cursor, base_path="D:/DataWarehousing/extract"):
    """
    Tạo config mới cho ngày tiếp theo
    base_path: đường dẫn thư mục chứa file extract (mặc định: D:/DataWarehousing/extract)
    """
    # Tính ngày tiếp theo
    tomorrow = datetime.now() + timedelta(days=1)
    date_str = tomorrow.strftime("%d-%m-%Y")
    date_only = tomorrow.strftime("%Y-%m-%d")
    
    # Tạo tên config và đường dẫn file
    config_name = f"Load_Agoda_{tomorrow.strftime('%Y_%m_%d')}"
    file_path = f"{base_path}/reviews_{date_str}.json"
    
    insert_query = """
        INSERT INTO conf.loadToStagingConf (
            config_name, file_path, file_format, created_by,
            extract_date, status, is_active, target_table, retry_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.execute(insert_query, (
        config_name,  # config_name
        file_path,  # file_path
        'review_dd-mm-yyyy.JSON',  # file_format
        'admin',  # created_by
        date_only,  # extract_date (YYYY-MM-DD)
        'PENDING',  # status
        True,  # is_active
        'staging.rawReviews',  # target_table
        0  # retry_count
    ))
    
    return config_name, file_path, date_only


# -----------------------------
def insert_raw_reviews(cursor, reviews):
    insert_query = """
        INSERT INTO staging.rawReviews(
            hotel_id, hotel_name, hotel_address, hotel_rating,
            review_id, reviewer_id, reviewer_name, demographic_name,
            checkin_date, checkout_date, review_title, review_original_title,
            positive, negative, review_text, review_original_text,
            rating, rating_text, review_date, language,
            country_id, country, country_iso2, response_info, source
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    total = 0
    for review in reviews:
        # Extract hotel info (same for all comments)
        hotel_id = review.get("propertyId")
        hotel_name = review.get("name")
        hotel_address = review.get("address")
        hotel_rating = review.get("hotel_score")
        source = review.get("source", "Unknown")
        
        # Iterate through comments array
        comments = review.get("reviews", {}).get("comments", [])
        for comment in comments:
            reviewer_info = comment.get("reviewerInfo", {})
            review_detail = comment.get("reviewDetail", {})
            rating_info = comment.get("rating", {})
            country_info = reviewer_info.get("country", {})
            
            cursor.execute(insert_query, (
                hotel_id,  # hotel_id
                hotel_name,  # hotel_name
                hotel_address,  # hotel_address
                hotel_rating,  # hotel_rating
                comment.get("id"),  # review_id
                comment.get("providerId"),  # reviewer_id
                reviewer_info.get("name"),  # reviewer_name
                reviewer_info.get("demographicName"),  # demographic_name
                reviewer_info.get("checkInDate"),  # checkin_date
                reviewer_info.get("checkOutDate"),  # checkout_date
                review_detail.get("title"),  # review_title
                review_detail.get("originalTitle"),  # review_original_title
                review_detail.get("positive"),  # positive
                review_detail.get("negative"),  # negative
                review_detail.get("comment"),  # review_text
                review_detail.get("originalComment"),  # review_original_text
                rating_info.get("score"),  # rating
                rating_info.get("scoreText"),  # rating_text
                review_detail.get("date"),  # review_date
                review_detail.get("languageId"),  # language
                country_info.get("id"),  # country_id
                country_info.get("name"),  # country
                country_info.get("countryIso2"),  # country_iso2
                str(comment.get("responseInfo")),  # response_info
                source  # source
            ))
            
            total += 1

    return total


# -----------------------------
# 5. MAIN PROCESS (WORKFLOW)
# -----------------------------
def main():
    conn = get_connection()
    cursor = conn.cursor()
    config_id = None
    log_id = None

    try:
        # ========== STEP 1: Query loadToStagingConf ==========
        cursor.execute("""
            SELECT c.config_id, c.file_path, c.extract_date
            FROM conf.loadToStagingConf c
            WHERE c.is_active = 1 
                AND c.status = 'PENDING'
                AND c.extract_date = (
                    SELECT MAX(extract_date)
                    FROM conf.loadToStagingConf
                    WHERE is_active = 1 
                        AND extract_date IN (
                            SELECT extract_date
                            FROM logs.extractLog
                            WHERE status = 'SUCCESS'
                        )
                )
            ORDER BY c.config_id LIMIT 1
        """)
        row = cursor.fetchone()

        # ========== STEP 2: Query extractLog ==========
        if row:
            config_id, file_path, extract_date = row
            file_name = os.path.basename(file_path)
            
            cursor.execute("""
                SELECT log_id, status, extract_date
                FROM logs.extractLog
                WHERE extract_date = %s AND status = 'SUCCESS'
                LIMIT 1
            """, (extract_date,))
            extract_log_row = cursor.fetchone()
            
            if not extract_log_row:
                print(f"❌ FAILED: No successful extract log found for date {extract_date}")
                return
        else:
            print("❌ FAILED: Không có config nào cần chạy với extract date gần nhất thành công.")
            return

        # ========== STEP 3: Check record ==========
        # Validate extract log exists
        if not extract_log_row:
            print(f"❌ FAILED: Extract log record not found for {extract_date}")
            # 3a1: Update config status to 'error'
            cursor.execute("""
                UPDATE conf.loadToStagingConf
                SET status = 'FAILED', updated_at = NOW()
                WHERE config_id = %s
            """, (config_id,))
            conn.commit()
            
            # 3a2: Add error log
            error_msg = f"No extract log found for {extract_date}"
            cursor.execute("""
                INSERT INTO logs.loadToStagingLog (config_id, status, message, file_processed, created_at)
                VALUES (%s, %s, %s, %s, CURDATE())
            """, (config_id, 'FAILED', error_msg, file_name))
            conn.commit()
            return

        # ========== STEP 5: Check file ==========
        if not os.path.exists(file_path):
            print(f"❌ FAILED: File not found: {file_path}")
            # 5a1: Update config status to 'error'
            cursor.execute("""
                UPDATE conf.loadToStagingConf
                SET status = 'FAILED', updated_at = NOW()
                WHERE config_id = %s
            """, (config_id,))
            conn.commit()
            
            # 5a2: Add error log
            error_msg = f"Failed to load file {file_name}"
            cursor.execute("""
                INSERT INTO logs.loadToStagingLog (config_id, status, message, file_processed, created_at)
                VALUES (%s, %s, %s, %s, CURDATE())
            """, (config_id, 'FAILED', error_msg, file_name))
            conn.commit()
            return

        # ========== STEP 7: Add row to loadToStagingLog with status='loading' ==========
        log_id, start_time = insert_log_start(cursor, config_id, file_name)
        conn.commit()

        # ========== STEP 8: Load JSON and insert to rawReviews ==========
        print(f"📂 Loading file: {file_path}")
        raw_data = load_json_data(file_path)
        
        # Truncate table
        cursor.execute("TRUNCATE TABLE staging.rawReviews")
        conn.commit()
        
        # Insert data
        total_records = insert_raw_reviews(cursor, raw_data)
        conn.commit()
        
        print(f"✅ Loaded {total_records} reviews")

        # ========== STEP 9: Update config status to 'done' ==========
        cursor.execute("""
            UPDATE conf.loadToStagingConf
            SET status = 'DONE', updated_at = NOW()
            WHERE config_id = %s
        """, (config_id,))
        conn.commit()

        # ========== STEP 10: Add row to loadToStagingLog with status='success' ==========
        update_log_end(cursor, log_id, total_records, "SUCCESS", "Load to staging complete")
        conn.commit()

        print(f"✔ DONE — Đã nạp {total_records} dòng.")

        # ========== Create next day config ==========
        new_config_name, new_file_path, new_extract_date = insert_next_day_config(cursor)
        conn.commit()
        print(f"📅 Created config for next day: {new_config_name}")

    except FileNotFoundError as e:
        print(f"❌ ERROR: File not found - {e}")
        if config_id and log_id:
            conn.rollback()
            # 8b1: Update config status to 'error'
            cursor.execute("""
                UPDATE conf.loadToStagingConf
                SET status = 'FAILED', updated_at = NOW()
                WHERE config_id = %s
            """, (config_id,))
            conn.commit()
            # 8b2: Add error log
            update_log_end(cursor, log_id, 0, "FAILED", f"Failed to load file: {str(e)}")
            conn.commit()
            # Create next day config anyway
            try:
                new_config_name, new_file_path, new_extract_date = insert_next_day_config(cursor)
                conn.commit()
            except:
                pass

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        if config_id:
            conn.rollback()
            if log_id:
                # 8b1: Update config status to 'error'
                cursor.execute("""
                    UPDATE conf.loadToStagingConf
                    SET status = 'FAILED', updated_at = NOW()
                    WHERE config_id = %s
                """, (config_id,))
                conn.commit()
                # 8b2: Add error log
                update_log_end(cursor, log_id, 0, "FAILED", f"Failed to load to rawReviews table: {str(e)}")
                conn.commit()
            
            # Create next day config anyway
            try:
                new_config_name, new_file_path, new_extract_date = insert_next_day_config(cursor)
                conn.commit()
            except:
                pass

    finally:
        cursor.close()
        conn.close()


# -----------------------------
# START
# -----------------------------
if __name__ == "__main__":
    main()