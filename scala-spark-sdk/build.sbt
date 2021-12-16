organization := "software.amazon.sagemaker.featurestore"
organizationName := "Amazon Web Services"
organizationHomepage := Some(url("https://aws.amazon.com"))
name := "sagemaker-feature-store-spark-sdk"
description := "This library provides a connector to Amazon SageMaker FeatureStore, allowing " +
  "customers to easily ingest data in scale to online/offline store."

homepage := Some(url("https://github.com/aws/sagemaker-feature-store-spark"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/aws/sagemaker-feature-store-spark"),
    "https://github.com/aws/sagemaker-feature-store-spark.git"
  )
)
licenses := Seq("Apache License, Version 2.0" -> url("https://aws.amazon.com/apache2.0"))

lazy val SageMakerFeatureStoreSpark = (project in file("."))
val sparkVersion = System.getProperty("SPARK_VERSION", "3.1.2")
version := {
 val base = baseDirectory.in(SageMakerFeatureStoreSpark).value
 IO.read(base / ".." / "VERSION").trim
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

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

jacocoReportSettings := JacocoReportSettings()
  .withThresholds(
    JacocoThresholds(
      line = 90)
  )

publishMavenStyle := true
pomIncludeRepository := { _ => false }
publishArtifact in Test := false
val nexusUriHost = "aws.oss.sonatype.org"
val nexusUriHostWithScheme = "https://" + nexusUriHost + "/"
val snapshotUrl = nexusUriHostWithScheme + "content/repositories/snapshots"
val localReleaseUrl = nexusUriHostWithScheme + "service/local"
val releaseUrl = localReleaseUrl + "/staging/deploy/maven2"

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at snapshotUrl)
  else
    Some("releases" at releaseUrl)
}

Sonatype.SonatypeKeys.sonatypeRepository := localReleaseUrl
Sonatype.SonatypeKeys.sonatypeCredentialHost := nexusUriHost

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  nexusUriHost,
  sys.env.getOrElse("SONATYPE_USERNAME", "NOT_A_PUBLISH_BUILD"),
  sys.env.getOrElse("SONATYPE_PASSWORD", "NOT_A_PUBLISH_BUILD")
)
pomExtra := (
  <developers>
    <developer>
      <id>amazonwebservices</id>
      <organization>Amazon Web Services</organization>
      <organizationUrl>https://aws.amazon.com</organizationUrl>
      <roles>
        <role>developer</role>
      </roles>
    </developer>
  </developers>
  )