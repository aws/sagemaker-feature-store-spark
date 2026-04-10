# SageMaker FeatureStore Spark SDK (Scala)

Scala library for ingesting data into [Amazon SageMaker FeatureStore](https://aws.amazon.com/sagemaker/feature-store/) from Spark DataFrames.

## Prerequisites

- JDK 11 or 17 (must match your target Spark version)
- [sbt](https://www.scala-sbt.org/)
- Scala 2.12

## Building

The SDK supports cross-building for Spark 3.1 through 3.5. Specify the target Spark version with `-DSPARK_VERSION`:

```bash
# Build fat JAR (includes all dependencies, shaded)
sbt -DSPARK_VERSION=3.5.1 assembly

# Default Spark version is 3.3.4 if not specified
sbt assembly
```

The fat JAR is written to `assembly-output/`.

To build a thin JAR (without dependencies):

```bash
sbt -DSPARK_VERSION=3.5.1 package
```

## Testing

Run unit tests with coverage and format check:

```bash
sbt jacoco scalafmtCheckAll
```

This runs:
- All unit tests via TestNG
- JaCoCo code coverage report (90% line coverage threshold)
- scalafmt format validation

The coverage report is generated at `target/scala-2.12/jacoco/report/`.

## Formatting

Auto-format all source files:

```bash
sbt scalafmtAll
```

## Using with PySpark

After building the fat JAR, copy it to the PySpark SDK jars directory:

```bash
cp assembly-output/sagemaker-feature-store-spark-sdk-assembly-*.jar \
   ../pyspark-sdk/src/feature_store_pyspark/jars/sagemaker-feature-store-spark-sdk-3.5.jar
```

Adjust the target filename suffix (`3.5`) to match your Spark major version.

## Assembly Notes

- Dependencies are shaded under `smfs.shaded.*` to avoid classpath conflicts
- Spark, Hadoop, and SLF4J are marked `Provided` (supplied by the runtime environment)
- The merge strategy discards `META-INF` and `org/slf4j` classes to avoid conflicts with PySpark's SLF4J 2.x binding
