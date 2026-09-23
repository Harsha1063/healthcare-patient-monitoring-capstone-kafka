import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object PatientVitalStreaming {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Healthcare Patient Vital Monitoring")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.blockManager.port", "0")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val patientSchema = StructType(
      Seq(
        StructField("event_id", StringType, true),
        StructField("patient_id", StringType, true),
        StructField("device_id", StringType, true),
        StructField("heart_rate", IntegerType, true),
        StructField("temperature", DoubleType, true),
        StructField("oxygen", IntegerType, true),
        StructField("timestamp", StringType, true)
      )
    )

    val kafkaData = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "patient-vitals")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    val patients = kafkaData
      .selectExpr("CAST(value AS STRING) AS json_value")
      .select(from_json(col("json_value"), patientSchema).alias("patient"))
      .select("patient.*")
      .filter(col("event_id").isNotNull).dropDuplicates("event_id")

    val alerts = patients.withColumn(
      "status",
      when(
        col("heart_rate") > 120 && col("oxygen") < 90 && col("temperature") > 39,
        "CRITICAL: HIGH_HEART_RATE, LOW_OXYGEN, HIGH_TEMPERATURE"
      )
      .when(
        col("heart_rate") > 120 && col("oxygen") < 90,
        "ALERT: HIGH_HEART_RATE, LOW_OXYGEN"
      )
      .when(
        col("heart_rate") > 120 && col("temperature") > 39,
        "ALERT: HIGH_HEART_RATE, HIGH_TEMPERATURE"
      )
      .when(
        col("oxygen") < 90 && col("temperature") > 39,
        "ALERT: LOW_OXYGEN, HIGH_TEMPERATURE"
      )
      .when(col("heart_rate") > 120, "ALERT: HIGH_HEART_RATE")
      .when(col("oxygen") < 90, "ALERT: LOW_OXYGEN")
      .when(col("temperature") > 39, "ALERT: HIGH_TEMPERATURE")
      .otherwise("NORMAL")
    )

    val finalResults = alerts.select(
      "event_id",
      "patient_id",
      "device_id",
      "heart_rate",
      "temperature",
      "oxygen",
      "timestamp",
      "status"
    )

    println("\n========== HEALTHCARE PATIENT VITAL MONITORING ==========\n")
    finalResults.show(false)

    val outputDir = Paths.get("../data/processed")
    Files.createDirectories(outputDir)

    val outputFile = outputDir.resolve("patient-alerts.csv")

    val rows = finalResults.collect()

    val header = "event_id,patient_id,device_id,heart_rate,temperature,oxygen,timestamp,status"

    val csvRows = rows.map { row =>
      val values = (0 until row.length).map { i =>
        "\"" + row.get(i).toString.replace("\"", "\"\"") + "\""
      }
      values.mkString(",")
    }

    Files.write(
      outputFile,
      (header +: csvRows).mkString(System.lineSeparator()).getBytes(StandardCharsets.UTF_8)
    )

    println("\nCSV FILE CREATED SUCCESSFULLY")
    println("Output: " + outputFile.toAbsolutePath)

    spark.stop()
  }
}

