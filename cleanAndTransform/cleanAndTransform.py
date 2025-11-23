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


# INSERT LOG
def insert_log(status, message):
    conn = get_conn("logs")
    with conn.cursor() as cur:
        sql = """INSERT INTO cleanAndTransformLog(process_name, start_time, status, message)
                 VALUES(%s, NOW(), %s, %s)"""
        cur.execute(sql, ("clean_transform", status, message))
        conn.commit()
    conn.close()

# LOAD CONFIG STATUS
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


# MAIN CLEAN & TRANSFORM PROCESS
def clean_and_transform():
    insert_log("running", "Start process clean & transform")

    conf = get_clean_conf()
    load_status = conf["load_status"]

    # 1. CHECK LOADING STATUS
    if load_status != "success":
        insert_log("failed", "Load to staging not successful – stop transform")
        update_clean_conf("failed")
        return

    # 2. LOAD RAW DATA
    conn = get_conn("Staging")
    raw = pd.read_sql("SELECT * FROM RawReviews", conn)

    if raw.empty:
        insert_log("success", "RawReviews empty – nothing to clean")
        update_clean_conf("done")
        conn.close()
        return

    update_clean_conf("cleaning")

    # 3. HANDLE MISSING VALUES
    raw = raw.dropna(subset=["reviewDate", "ratingScore"])

    # Replace empty text fields with empty string
    text_cols = ["title", "comment", "positive", "negative"]
    for col in text_cols:
        raw[col] = raw[col].fillna("")

    insert_log("cleaned", "Missing values handled")

    update_clean_conf("transforming")

    # 4. VALIDATE TYPES
    date_columns = ["checkinDate", "checkoutDate", "reviewDate"]
    for col in date_columns:
        raw[col] = pd.to_datetime(raw[col], errors="coerce")

    raw = raw.dropna(subset=["reviewDate"])

    # rating float
    raw["ratingScore"] = raw["ratingScore"].astype(float)

    # 5. TRANSFORM TEXT (example)
    raw["title"] = raw["title"].apply(lambda x: x.strip())
    raw["comment"] = raw["comment"].apply(lambda x: x.strip())

    insert_log("transformed", "Data transformed successfully")

    # 6. TRUNCATE CLEAN TABLE
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE CleanAndTransformed")
        conn.commit()
    # 7. LOAD TRANSFORMED DATA
    insert_sql = """
        INSERT INTO CleanAndTransformed (
            hotelName, address, avg_score,
            review_id, review_score, title,
            positive, negative,
            checkInDate, checkOutDate, reviewerName,
            countryID, countryName, countryISO2,
            guestType, languageID, languageName
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    data = raw[[
        "name", "demographicName", "ratingScore",
        "id", "ratingScore", "title",
        "positive", "negative",
        "checkinDate", "checkoutDate", "name",
        "countryId", "countryName", "countryIso2",
        "demographicName", "providerId", "providerId"
    ]].values.tolist()

    with conn.cursor() as cur:
        cur.executemany(insert_sql, data)
        conn.commit()

    conn.close()

    update_clean_conf("done")
    insert_log("success", "Clean & Transform completed successfully")


# RUN
if __name__ == "__main__":
    clean_and_transform()
