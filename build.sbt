name := "Travel-Audience Challenge"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.9.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test
)
