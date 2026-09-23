import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object PatientEventProducer {

  def main(args: Array[String]): Unit = {

    val topic = "patient-vitals"

    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", classOf[StringSerializer].getName)
    properties.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](properties)

    val events = Seq(
      (
        "P100",
        """{"event_id":"EVT200001","patient_id":"P100","device_id":"DEV001","heart_rate":78,"temperature":36.8,"oxygen":98,"timestamp":"2026-09-23 17:15:00"}"""
      ),
      (
        "P101",
        """{"event_id":"EVT200002","patient_id":"P101","device_id":"DEV002","heart_rate":145,"temperature":37.1,"oxygen":97,"timestamp":"2026-09-23 17:16:00"}"""
      ),
      (
        "P102",
        """{"event_id":"EVT200003","patient_id":"P102","device_id":"DEV003","heart_rate":145,"temperature":39.2,"oxygen":88,"timestamp":"2026-09-23 17:17:00"}"""
      )
    )

    try {
      events.foreach { case (patientId, jsonEvent) =>

        val record = new ProducerRecord[String, String](
          topic,
          patientId,
          jsonEvent
        )

        producer.send(record)

        println(s"Sent event for patient $patientId")
        println(jsonEvent)
        println("----------------------------------------")
      }

      producer.flush()

      println("All patient events sent successfully.")

    } finally {
      producer.close()
    }
  }
}