name := "direct_kafka_word_count"

scalaVersion := "2.10.5"
val kafkaVersion = "0.8.2.1"

val sparkVersion = "1.6.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % kafkaVersion % "provided",
  "org.apache.kafka" % "kafka-clients" % kafkaVersion % "provided",
  "com.databricks" % "spark-xml_2.10" % "0.3.3" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided",
  "org.json" % "json" % "20160212" % "provided",
  ("org.apache.spark" %% "spark-streaming-kafka" % sparkVersion) exclude ("org.spark-project.spark", "unused")
)



resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Spray repository"    at "http://repo.spray.io/",
  "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
)

resolvers += "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases/"

resolvers += Resolver.sonatypeRepo("releases")

resolvers += Resolver.sonatypeRepo("snapshots")


assemblyJarName in assembly := name.value + ".jar"
