import pymysql
import pandas as pd
from datetime import datetime

# CONNECT TO DATABASES
def get_conn(db_name):
    return pymysql.connect(
        host="127.0.0.1",
        user="staging",
        password="123",
        db=db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

# LOGGING
def insert_log(status, message):
    conn = get_conn("logs")
    with conn.cursor() as cur:
        sql = """
        INSERT INTO cleanAndTransformLog(process_name, start_time, status, message)
        VALUES(%s, NOW(), %s, %s)
        """
        cur.execute(sql, ("clean_transform", status, message))
        conn.commit()
    conn.close()


# CONFIG PROCESS
def get_clean_conf():
    conn = get_conn("conf")
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM cleanAndTransformConf LIMIT 1")
        row = cur.fetchone()
    conn.close()
    return row

def update_clean_conf(status):
    conn = get_conn("conf")
    with conn.cursor() as cur:
        cur.execute("UPDATE cleanAndTransformConf SET status=%s", (status,))
        conn.commit()
    conn.close()

# SAFE TYPE FUNCTIONS
def safe_date(x):
    try:
        return pd.to_datetime(x, errors="coerce")
    except:
        return pd.NaT

def safe_float(x):
    try:
        return float(x)
    except:
        return None

# MAIN CLEAN & TRANSFORM
def clean_and_transform():
    insert_log("running", "Start Clean & Transform")

    conf = get_clean_conf()
    load_status = conf["load_status"]

    # 1. CHECK LOADING STATUS
    if load_status != "SUCCESS" and load_status != "success":
        insert_log("failed", "Load staging not successful → stop transform")
        update_clean_conf("FAILED")
        return

    # 2. LOAD RAW DATA FROM STAGING
    conn = get_conn("Staging")
    raw = pd.read_sql("SELECT * FROM rawReviews", conn)

    if raw.empty:
        insert_log("success", "RawReviews empty – nothing to clean")
        update_clean_conf("DONE")
        conn.close()
        return

    update_clean_conf("CLEANING")
    # 3. CLEAN MISSING VALUES

    # Remove rows missing essential values
    raw = raw.dropna(subset=["review_id", "rating", "review_date"])

    # Replace empty text fields
    text_fields = ["review_title", "positive", "negative", "review_text"]
    for col in text_fields:
        raw[col] = raw[col].fillna("")

    insert_log("cleaned", "Missing values handled")

    update_clean_conf("TRANSFORMING")

    # 4. VALIDATE TYPES

    # Convert dates
    raw["review_date"] = raw["review_date"].apply(safe_date)
    raw["checkin_date"] = raw["checkin_date"].apply(safe_date)
    raw["checkout_date"] = raw["checkout_date"].apply(safe_date)

    # Remove invalid dates
    raw = raw[raw["review_date"].notna()]

    # Convert rating to float
    raw["rating"] = raw["rating"].apply(safe_float)

    insert_log("validated", "Type validation completed")


    # 5. TRANSFORM TEXT
    raw["review_title"] = raw["review_title"].apply(lambda x: x.strip())
    raw["review_text"] = raw["review_text"].apply(lambda x: x.strip())

    insert_log("transformed", "Text transformed successfully")

    # 6. TRUNCATE CLEAN TABLE

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE CleanAndTransformed")
        conn.commit()

  
    # 7. INSERT INTO CLEAN TABLE

    insert_sql = """
        INSERT INTO CleanAndTransformed (
            hotelName, address, avg_score,
            review_id, review_score, title,
            positive, negative,
            checkInDate, checkOutDate, reviewerName,
            countryID, countryName, countryISO2,
            guestType, languageID
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    # Mapping staging.rawReviews
    data = raw[[
        "hotel_name",       
        "hotel_address",    
        "hotel_rating",     
        "review_id",        
        "rating",           
        "review_title",     
        "positive",        
        "negative",         
        "checkin_date",     
        "checkout_date",   
        "reviewer_name",    
        "country_id",       
        "country",          
        "country_iso2",     
        "demographic_name",
        "language"          
    ]].values.tolist()

    with conn.cursor() as cur:
        cur.executemany(insert_sql, data)
        conn.commit()

    conn.close()

    update_clean_conf("DONE")
    insert_log("success", "Clean & Transform completed successfully")


# START
if __name__ == "__main__":
    clean_and_transform()
