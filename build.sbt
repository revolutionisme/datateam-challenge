name := "Travel-Audience Challenge"

version := "0.1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.9.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test"
)
