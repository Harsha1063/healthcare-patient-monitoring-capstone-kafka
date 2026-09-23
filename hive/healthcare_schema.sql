CREATE DATABASE IF NOT EXISTS healthcare;

USE healthcare;

CREATE TABLE IF NOT EXISTS patient_vitals (
    event_id STRING,
    patient_id STRING,
    device_id STRING,
    heart_rate INT,
    temperature DOUBLE,
    oxygen INT,
    timestamp STRING,
    status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS patient_alerts (
    event_id STRING,
    patient_id STRING,
    device_id STRING,
    heart_rate INT,
    temperature DOUBLE,
    oxygen INT,
    timestamp STRING,
    alert_type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
