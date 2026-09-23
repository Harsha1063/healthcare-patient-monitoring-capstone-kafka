# Healthcare Patient Vital Monitoring & Real-Time Alerting System

## Architecture

Patient Sensors
      |
      v
    Kafka
      |
      v
    Spark
      |
      v
 Healthcare Rule Engine
      |
      +----> CSV Alert Storage
      |
      +----> Hive Schema
      |
      +----> HBase Schema

## Technologies

- Apache Kafka 4.3.1
- Apache Spark 3.5.6
- Scala 2.12.18
- SBT 1.10.11
- Java 17
- Hive schema
- HBase schema

## Kafka Topic

patient-vitals

## Patient Vital Rules

- Heart rate > 120 -> HIGH_HEART_RATE
- Oxygen < 90 -> LOW_OXYGEN
- Temperature > 39 -> HIGH_TEMPERATURE

## Processing

Kafka events are read by Spark, JSON data is parsed into structured patient-vital records, and healthcare rules classify each event as NORMAL, ALERT, or CRITICAL.

## Output

Processed alerts are stored at:

data/processed/patient-alerts.csv

## Sample Critical Event

Patient: P100
Heart Rate: 145
Temperature: 39.2
Oxygen: 88

Result:
CRITICAL: HIGH_HEART_RATE, LOW_OXYGEN, HIGH_TEMPERATURE

## Project Structure

data/
  processed/
    patient-alerts.csv

hive/
  healthcare_schema.sql

hbase/
  healthcare_hbase_schema.txt

kafka/

producer/

spark-streaming/
  src/main/scala/
    PatientVitalStreaming.scala

## Environment Note

The Spark processing pipeline is working successfully on Windows.

Windows Hadoop NativeIO caused filesystem-write and Structured Streaming checkpoint issues, so the working demonstration uses Kafka batch consumption and Java file I/O for CSV persistence.

Hive and HBase schema definitions are included for the intended production architecture.
