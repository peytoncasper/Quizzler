name := "quizzlerAlerts"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.1"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.1"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"

lazy val root = (project in file(".")).enablePlugins(PlayScala)