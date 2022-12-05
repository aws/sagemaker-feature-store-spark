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

// change the output path of assembly jar
lazy val SageMakerFeatureStoreSpark = (project in file(".")).settings(
  assembly / assemblyOutputPath := file(s"./assembly-output/${(assembly/assemblyJarName).value}")
)

val sparkVersion = System.getProperty("SPARK_VERSION", "3.1.2")
val majorSparkVersion = sparkVersion.substring(0, sparkVersion.lastIndexOf("."))

val awsSDKVersion = "2.18.28"
val sparkVersionToHadoopVersionMap = Map(
  "3.1" -> "3.2.1",
  "3.2" -> "3.2.1",
  "3.3" -> "3.2.1",
)

// read the version number
version := {
  val base = (SageMakerFeatureStoreSpark / baseDirectory).value
  IO.read(base / ".." / "VERSION").trim
}

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  // SDK v2 is required by iceberg and spark connector. Since some platforms do not provide these dependencies, we
  // pack them up and provide for users
  "software.amazon.awssdk" % "sagemaker" % awsSDKVersion,
  //  "software.amazon.awssdk" % "sagemakerfeaturestoreruntime" % awsSDKVersion,

  "software.amazon.awssdk" % "glue" % awsSDKVersion,
  "software.amazon.awssdk" % "s3" % awsSDKVersion,
  "software.amazon.awssdk" % "dynamodb" % awsSDKVersion,
  "software.amazon.awssdk" % "kms" % awsSDKVersion,
  "software.amazon.awssdk" % "sts" % awsSDKVersion,
  "software.amazon.awssdk" % "url-connection-client" % awsSDKVersion,

  "org.apache.iceberg" %% s"iceberg-spark-runtime-$majorSparkVersion" % "0.14.+",

  // hadoop-common and hadoop-aws should be provided by either platform or user. On EMR, sagemaker processing these are
  // pre-installed, to avoid dependency conflict which could cause weird failures, we exclude them from fat jar. Besides
  // spark has a tight coupling with the version of hadoop.
  "org.apache.hadoop" % "hadoop-aws" % sparkVersionToHadoopVersionMap(majorSparkVersion) % Provided,
  "org.apache.hadoop" % "hadoop-common" %  sparkVersionToHadoopVersionMap(majorSparkVersion) % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,

  "org.mockito" %% "mockito-scala-scalatest" % "1.17.12" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalatestplus" %% "testng-6-7" % "3.2.9.0" % Test,
)

exportJars := true
lazy val printClasspath = taskKey[Unit]("Dump classpath")
printClasspath := (Runtime / fullClasspath value) foreach { e => println(e.data) }

assembly / assemblyMergeStrategy := {
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
Test / publishArtifact := false
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

pomExtra := <developers>
  <developer>
    <id>amazonwebservices</id>
    <organization>Amazon Web Services</organization>
    <organizationUrl>https://aws.amazon.com</organizationUrl>
    <roles>
      <role>developer</role>
    </roles>
  </developer>
</developers>