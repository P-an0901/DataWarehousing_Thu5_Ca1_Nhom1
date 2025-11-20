INSERT INTO conf.extractConf 
(conf_id, api_url)
values
(1, 'abc'),
(2, 'abc'),
(3, 'abc'),
(4, 'abc'),
(5, 'abc');

INSERT INTO conf.loadToStagingConf 
(config_name, file_path, file_format, created_by, extract_date, status, is_active, target_table, retry_count)
VALUES
('Load_Agoda_2025_11_12', 'D:/DataWarehousing/extract/reviews_20-11-2025.json', 'review_dd-mm-yyyy.JSON', 'admin', '2025-11-20', 'PENDING', TRUE, 'staging.rawReviews', 0),
('Load_RapidAPI_2025_11_12', 'D:/DataWarehousing/extract/reviews_20-11-2025.json', 'review_dd-mm-yyyy.JSON', 'admin', '2025-11-20', 'FAILED', TRUE, 'staging.rawReviews', 2),
('Load_Agoda_2025_11_13', 'D:/DataWarehousing/extract/reviews_20-11-2025.json', 'review_dd-mm-yyyy.JSON', 'system', '2025-11-20', 'RUNNING', TRUE, 'staging.rawReviews', 0),
('Load_Test_Inactive', 'D:/DataWarehousing/extract/reviews_20-11-2025.json', 'review_dd-mm-yyyy.JSON', 'tester', '2025-11-20', 'FAILED', FALSE, 'staging.rawReviews', 1),
('Load_Corrupted', 'D:/DataWarehousing/extract/reviews_20-11-2025.json', 'review_dd-mm-yyyy.JSON', 'auto', '2025-11-20', 'FAILED', TRUE, 'staging.rawReviews', 3);

INSERT INTO logs.extractLog (conf_id, extract_date, status, message)
VALUES
(1, '2025-11-20', 'SUCCESS', 'Extracted successfully from source Agoda'),
(2, '2025-11-21', 'FAILED', 'Connection error from RapidAPI'),
(3, '2025-11-22', 'SUCCESS', 'Extract completed normally'),
(4, '2025-11-23', 'SUCCESS', 'Manual extract test'),
(5, '2025-11-24', 'FAILED', 'JSON invalid format');

INSERT INTO logs.loadToStagingLog
(config_id, start_time, end_time, status, total_records, message, file_processed, created_at)
VALUES
(1, '2025-11-12 08:00:00', '2025-11-12 08:02:00', 'SUCCESS', 200, 'Loaded successfully', 'review_12_11.json', '2025-11-12'),

(2, '2025-11-12 08:05:00', '2025-11-12 08:06:00', 'FAILED', 0, 'Missing rating field', 'review_12_11.json', '2025-11-12'),

(2, '2025-11-12 08:07:00', '2025-11-12 08:09:00', 'FAILED', 0, 'Retry #2: invalid JSON format', 'review_12_11.json', '2025-11-12'),

(3, '2025-11-13 09:00:00', NULL, 'RUNNING', 0, 'System is loading…', 'review_13_11.json', '2025-11-13'),

(5, '2025-11-11 07:00:00', '2025-11-11 07:03:00', 'FAILED', 0, 'Corrupted JSON file', 'review_corrupt.json', '2025-11-11');

INSERT INTO staging.rawReviews
(review_id, hotel_id, hotel_name, reviewer_name, review_text, rating, review_date, language, country, source)
VALUES
-- Dữ liệu hợp lệ
('R001', 'H1001', 'The Sunrise Hotel', 'Nguyen Van A', 'Phòng sạch sẽ', '4.5', '12/11/2025', 'vi', 'Vietnam', 'Agoda'),
('R002', 'H1002', 'Skyline Resort', 'John Smith', 'Great location', '4.8', '11/11/2025', 'en', 'USA', 'RapidAPI'),

-- Rating lỗi
('R003', 'H1003', 'Beach Paradise', 'Maria Lopez', 'Bad service', 'N/A', '11/11/2025', 'en', 'Spain', 'RapidAPI'),

-- Thiếu review_text
('R004', 'H1004', 'Golden Palace', 'Akira Y', NULL, '3.5', '10/11/2025', 'jp', 'Japan', 'Agoda'),

-- Ngày sai định dạng
('R005', 'H1005', 'Budget Stay', 'Peter Nguyen', 'Good price', '4.0', '31-11-2025', 'vi', 'Vietnam', 'RapidAPI'),

-- Lỗi ký tự Unicode
('R006', 'H1006', 'Mountain View', 'Li Wei', '房间非常干净', '5', '12/11/2025', 'zh', 'China', 'Agoda'),

-- Lỗi trống toàn bộ
('R007', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Agoda'),

-- Review trùng
('R008', 'H1007', 'Sky View', 'Sara Tan', 'Nice decoration', '4.7', '12/11/2025', 'en', 'Singapore', 'Agoda'),
('R008', 'H1007', 'Sky View', 'Sara Tan', 'Nice decoration', '4.7', '12/11/2025', 'en', 'Singapore', 'Agoda');