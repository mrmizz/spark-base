name := "spark-base"
organization := "in.tap"
version := "1.0.0-SNAPSHOT"
description := "build your spark project on top of this."

publishMavenStyle := true

scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-Xfatal-warnings",
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)

val versionSpark: String = "2.4.0"

libraryDependencies ++= Seq(
  // arg parser
  "org.rogach" %% "scallop" % "3.1.5",
  // test resources
  "org.scalactic" % "scalactic_2.11" % "3.0.4",
  "org.scalatest" % "scalatest_2.11" % "3.0.4",
  // apache spark
  "org.apache.spark" %% "spark-core" % versionSpark % Provided,
  "org.apache.spark" %% "spark-sql" % versionSpark % Provided
)
