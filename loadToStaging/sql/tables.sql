-- Database cho quá trình Staging
CREATE DATABASE IF NOT EXISTS Staging;
Drop table if exists logs.loadToStagingLog;
Drop table if exists logs.extractLog;
Drop table if exists staging.rawReviews;
Drop table if exists conf.loadToStagingConf;
Drop table if exists conf.extractConf;

-- Bảng cấu hình
CREATE TABLE conf.extractConf (
    conf_id INT AUTO_INCREMENT PRIMARY KEY,
    api_url VARCHAR(500) NOT NULL,
    api_key VARCHAR(255),
    api_host VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE conf.loadToStagingConf (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    config_name VARCHAR(100) NOT NULL,
    file_path VARCHAR(255) NOT NULL,
    file_format VARCHAR(50) DEFAULT 'review_dd-mm-yyyy.JSON',
    created_by VARCHAR(50) DEFAULT 'system',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    extract_date date,
    updated_at datetime default current_timestamp,
    status varchar(20) default 'PENDING', -- FAILED, RUNNING, NULL, PENDING
    is_active BOOLEAN DEFAULT TRUE,
    target_table varchar(50),
    retry_count INT
);

-- Bảng log
CREATE TABLE logs.extractLog (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    conf_id INT NOT NULL,
    extract_date date,
    status varchar(50), -- SUCCESS, FAILED
    message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conf_id) REFERENCES conf.extractConf(conf_id) ON DELETE CASCADE
);

CREATE TABLE logs.loadToStagingLog (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT,
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(20) DEFAULT 'PENDING',  -- RUNNING / SUCCESS / FAILED / PENDING
    total_records INT DEFAULT 0,
    message TEXT,
    file_processed varchar(200),
    created_at date,
    FOREIGN KEY (config_id) REFERENCES conf.loadToStagingConf(config_id)
);

-- Bảng dữ liệu review (raw)
CREATE TABLE staging.rawReviews (
    raw_id int PRIMARY KEY auto_increment,
    hotel_id VARCHAR(50),
    hotel_name VARCHAR(255),
    hotel_address varchar(255),
    hotel_rating varchar(10),
    review_id varchar(50),
    reviewer_id varchar(50),
    reviewer_name VARCHAR(255),
    demographic_name varchar(50),
    checkin_date varchar(20),
    checkout_date varchar(20),
    review_title text,
    review_original_title text,
    positive text,
    negative text,
    review_text TEXT,
    review_original_text text,
    rating varchar(10),
    rating_text varchar(50),
    review_date varchar(50),
    language VARCHAR(50),
    country_id varchar(10),
    country VARCHAR(100),
    country_iso2 varchar(10),
    response_info text,
    source VARCHAR(100),
    load_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
