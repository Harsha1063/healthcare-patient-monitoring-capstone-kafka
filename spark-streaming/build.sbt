ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.healthcare"
ThisBuild / version := "1.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "healthcare-patient-vital-streaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.6",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.6"
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
