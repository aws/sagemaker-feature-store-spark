#!/usr/bin/env python3
import os
import shutil
import sys
import subprocess

from setuptools import setup
from setuptools.command.install import install

SPARK_HOME = os.getenv('SPARK_HOME')
TEMP_PATH = "deps"
VERSION_PATH = "VERSION"
JARS_TARGET = os.path.join(TEMP_PATH, "jars")
in_spark_sdk = os.path.isfile("../scala-spark-sdk/build.sbt")

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def read_version():
    return read(VERSION_PATH).strip()


class CustomInstall(install):
    def run(self):
        install.run(self)
        spark_home_dir = os.environ.get('SPARK_HOME', None)
        if spark_home_dir:
            print("Copying depdendent jars to SPARK_HOME...")
            for jar in os.listdir(JARS_TARGET):
                target_path = os.path.join(spark_home_dir, "jars", os.path.basename(jar))
                source_path = os.path.join(JARS_TARGET, jar)
                shutil.copy(source_path, target_path)
        else:
            print("Environment variable SPARK_HOME is not set, dependent jars are not installed to SPARK_HOME.")
        print("Installation finished.")


print("Starting the installation of SageMaker FeatureStore pyspark...")
if in_spark_sdk:
    shutil.copyfile(os.path.join("..", VERSION_PATH), VERSION_PATH)

    if not os.path.exists(TEMP_PATH):
        os.mkdir(TEMP_PATH)
    p = subprocess.Popen("sbt printClasspath".split(),
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         cwd="../scala-spark-sdk/")

    output, errors = p.communicate()

    # Java Libraries to include.
    java_libraries = ['aws', 'sagemaker', 'hadoop', 'reactive-streams', 'guava']
    classpath = []
    for line in output.decode('utf-8').splitlines():
        path = str(line.strip())
        if path.endswith(".jar") and os.path.exists(path):
            jar = os.path.basename(path).lower()
            if any(lib in path for lib in java_libraries):
                classpath.append(path)

    if len(classpath) == 0:
        print("Failed to retrieve the jar classpath. Can't package")
        exit(-1)

    if not os.path.exists(JARS_TARGET):
        os.mkdir(JARS_TARGET)
    for jar in classpath:
        target_path = os.path.join(JARS_TARGET, os.path.basename(jar))
        shutil.copy(jar, target_path)

else:
    if not os.path.exists(JARS_TARGET):
        print("You need to be in the sagemaker-feature-store-spark root folder to package", file=sys.stderr)
        exit(-1)

setup(
    name="sagemaker_feature_store_pyspark",
    # version=read_version(),
    version="0.0.18",
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
