USE Conf;
CREATE TABLE `extractconf` (
  `conf_id` int NOT NULL AUTO_INCREMENT,
  `api_url` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL,
  `api_key` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_host` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `city_id` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`conf_id`)
); 
CREATE TABLE `extractProcess` (
  `process_id` int NOT NULL AUTO_INCREMENT,
  `conf_id` int NOT NULL,
  `start_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `end_time` datetime DEFAULT NULL,
  `status` enum('running','done','error') COLLATE utf8mb4_unicode_ci DEFAULT 'running',
  `remarks` varchar(1000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `extract_date` date NOT NULL,
  PRIMARY KEY (`process_id`),
  KEY `conf_id` (`conf_id`),
  CONSTRAINT `process_ibfk_1` FOREIGN KEY (`conf_id`) REFERENCES `extractconf` (`conf_id`)
);

USE Logs;
CREATE TABLE `extractlog` (
  `log_id` int NOT NULL AUTO_INCREMENT,
  `conf_id` int NOT NULL,
  `status` enum('info','warning','error','running','done','success') COLLATE utf8mb4_unicode_ci DEFAULT 'info',
  `message` text COLLATE utf8mb4_unicode_ci,
  `extract_date` date NOT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`log_id`)
)
