import os
import json
import mysql.connector
from datetime import datetime

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
# 4. INSERT vào staging.rawReviews
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

    # 1. Lấy config active
    cursor.execute("""
        SELECT config_id, file_path
        FROM conf.loadToStagingConf
        WHERE is_active = 1 AND status = 'PENDING'
        ORDER BY config_id LIMIT 1
    """)
    row = cursor.fetchone()

    if not row:
        print("Không có config nào cần chạy.")
        return

    config_id, file_path = row
    file_name = os.path.basename(file_path)

    # 2. Ghi log START
    log_id, start_time = insert_log_start(cursor, config_id, file_name)
    conn.commit()

    try:
        # 3. Load JSON
        raw_data = load_json_data(file_path)

        # 4. TRUNCATE bảng rawReviews (theo workflow mới)
        cursor.execute("TRUNCATE TABLE staging.rawReviews")
        conn.commit()

        # 5. Insert dữ liệu
        total_records = insert_raw_reviews(cursor, raw_data)
        conn.commit()

        # 6. Update trạng thái CONF
        cursor.execute("""
            UPDATE conf.loadToStagingConf
            SET status = 'DONE', updated_at = NOW()
            WHERE config_id = %s
        """, (config_id,))
        conn.commit()

        # 7. Ghi log SUCCESS
        update_log_end(cursor, log_id, total_records, "SUCCESS", "Load thành công.")
        conn.commit()

        print(f"✔ DONE — Đã nạp {total_records} dòng.")

    except Exception as e:
        conn.rollback()

        # 8. Log FAILED
        update_log_end(cursor, log_id, 0, "FAILED", str(e))
        conn.commit()

        cursor.execute("""
            UPDATE conf.loadToStagingConf
            SET status = 'FAILED', updated_at = NOW()
            WHERE config_id = %s
        """, (config_id,))
        conn.commit()

        print("❌ ERROR:", e)

    finally:
        cursor.close()
        conn.close()


# -----------------------------
# START
# -----------------------------
if __name__ == "__main__":
    main()