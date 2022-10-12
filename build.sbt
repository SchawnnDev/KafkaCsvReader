ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "ExoArchiReact"
  )

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "2.0.3",
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-streaming" % "3.3.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.2.3",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0",
  "joda-time" % "joda-time" % "2.11.1",
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % "8.4.2",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "8.4.2",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.4.1",
)