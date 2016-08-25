name := "QuizzlerTelegramIngestor"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M2"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0"
libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.4"
libraryDependencies += "com.typesafe.akka" % "akka-stream_2.11" % "2.4.4"
libraryDependencies += "com.typesafe.akka" % "akka-http-experimental_2.11" % "2.4.4"
libraryDependencies += "com.typesafe.akka" % "akka-http-core_2.11" % "2.4.4"
libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.3.0"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"
libraryDependencies += "org.jfarcand" % "wcs" % "1.5"

