organization := "com.amazonaws"
organizationName := "Amazon Web Services"
organizationHomepage := Some(url("https://aws.amazon.com"))
name := "sagemaker-feature-store-spark-sdk"

homepage := Some(url("https://github.com/aws/sagemaker-feature-store-spark"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/aws/sagemaker-feature-store-spark"),
    "https://github.com/aws/sagemaker-feature-store-spark.git"
  )
)
licenses := Seq("Apache License, Version 2.0" -> url("https://aws.amazon.com/apache2.0"))

lazy val SageMakerFeatureStoreSpark = (project in file("."))
val sparkVersion = System.getProperty("SPARK_VERSION", "3.1.1")
version := {
 val base = baseDirectory.in(SageMakerFeatureStoreSpark).value
 val packageVersion = IO.read(base / ".." / "VERSION").trim
 s"spark_$sparkVersion-$packageVersion"
}

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-aws" % "3.1.4",
  "org.apache.hadoop" % "hadoop-common" % "3.1.4",
  "software.amazon.awssdk" % "sagemaker" % "2.17.48",
  "software.amazon.awssdk" % "sagemakerfeaturestoreruntime" % "2.16.32",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.mockito" %% "mockito-scala-scalatest" % "1.16.37" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalatestplus" %% "testng-6-7" % "3.2.9.0" % Test
)

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.5",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.5",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.12.5"
  )
}

exportJars := true
lazy val printClasspath = taskKey[Unit]("Dump classpath")
printClasspath := (fullClasspath in Runtime value) foreach { e => println(e.data) }

scalafmtOnCompile := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

jacocoReportSettings := JacocoReportSettings()
  .withThresholds(
    JacocoThresholds(
      line = 90)
  )

// TODO: Add sonatype configuration