ThisBuild / scalaVersion := "2.12.18"

ThisBuild / organization := "com.healthcare"

ThisBuild / version := "1.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "healthcare-patient-vital-producer",

    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "4.3.1",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.2"
    )
  )
