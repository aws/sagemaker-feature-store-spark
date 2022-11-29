#!/usr/bin/env python3
import os
import shutil
import sys
import subprocess

from setuptools import setup
from setuptools.command.install import install
from pathlib import Path

SPARK_HOME = os.getenv('SPARK_HOME')
TEMP_PATH = "deps"
VERSION_PATH = "VERSION"
JARS_TARGET = os.path.join(TEMP_PATH, "jars")
SCALA_SPARK_DIR = Path("../scala-spark-sdk")
UBER_JAR_NAME_PREFIX = "sagemaker-feature-store-spark-sdk"
UBER_JAR_NAME = f"{UBER_JAR_NAME_PREFIX}.jar"

in_spark_sdk = os.path.isfile(SCALA_SPARK_DIR / "build.sbt")
# read the contents of your README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def read_version():
    return read(VERSION_PATH).strip()


# This is a post installation step. It will copy feature store spark uber jar to $SPARK_HOME/jars
class CustomInstall(install):
    def run(self):
        install.run(self)
        spark_home_dir = os.environ.get('SPARK_HOME', None)
        if spark_home_dir:
            uber_jar_target = Path(spark_home_dir) / "jars" / UBER_JAR_NAME

            jars_in_deps = os.listdir(Path(os.getcwd()) / Path(JARS_TARGET))
            uber_jar_name = [jar for jar in jars_in_deps if jar.startswith(UBER_JAR_NAME_PREFIX)].pop()
            uber_jar_dir = Path(os.getcwd()) / Path(JARS_TARGET) / uber_jar_name

            print(f"Copying feature store uber jar to {uber_jar_target}")
            shutil.copy(uber_jar_dir, uber_jar_target)

        else:
            print("Environment variable SPARK_HOME is not set, dependent jars are not installed to SPARK_HOME.")
        print("Installation finished.")


print("Starting the installation of SageMaker FeatureStore pyspark...")
if in_spark_sdk:
    shutil.copyfile(os.path.join("..", VERSION_PATH), VERSION_PATH)

    if not os.path.exists(TEMP_PATH):
        os.mkdir(TEMP_PATH)

    # use sbt to package the scala uber jar
    p = subprocess.Popen("sbt assembly".split(),
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         cwd=SCALA_SPARK_DIR)
    p.communicate()

    # retrieve all jars under 'assembly-output'
    classpath = []
    assembly_output_dir = SCALA_SPARK_DIR / "assembly-output"
    assembly_output_files = os.listdir(assembly_output_dir)
    for output_file in assembly_output_files:
        file_path = assembly_output_dir / output_file
        if output_file.endswith(".jar") and os.path.exists(file_path):
            classpath.append(file_path)

    if len(classpath) == 0:
        print("Failed to retrieve the jar classpath. Can't package")
        exit(-1)

    if not os.path.exists(JARS_TARGET):
        os.mkdir(JARS_TARGET)

    uber_jar_path = [jar for jar in classpath if os.path.basename(jar).startswith(UBER_JAR_NAME_PREFIX)].pop()
    target_path = os.path.join(JARS_TARGET, UBER_JAR_NAME)
    shutil.copy(uber_jar_path, target_path)

else:
    if not os.path.exists(JARS_TARGET):
        print("You need to be in the sagemaker-feature-store-spark root folder to package", file=sys.stderr)
        exit(-1)

setup(
    name="sagemaker_feature_store_pyspark_3.1",
    author="Amazon Web Services",

    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="ML Amazon AWS AI FeatureStore SageMaker",

    version=read_version(),
    description="Amazon SageMaker FeatureStore PySpark Bindings",
    license="Apache License 2.0",
    zip_safe=False,

    packages=["feature_store_pyspark",
              "feature_store_pyspark.jars"],

    package_dir={
        "feature_store_pyspark": "src/feature_store_pyspark",
        "feature_store_pyspark.jars": "deps/jars"
    },
    include_package_data=True,

    scripts=["bin/feature-store-pyspark-dependency-jars"],

    package_data={
        "feature_store_pyspark.jars": ["*.jar"],
    },

    install_requires=[],

    cmdclass={
        'install': CustomInstall
    }
)
