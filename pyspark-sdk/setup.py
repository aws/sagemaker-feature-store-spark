#!/usr/bin/env python3
import os
import shutil
import sys
import subprocess

from setuptools import setup
from setuptools.command.install import install
from pathlib import Path

TEMP_PATH = "deps"
VERSION_PATH = "VERSION"
JARS_TARGET = os.path.join(TEMP_PATH, "jars")
SCALA_SPARK_DIR = Path("../scala-spark-sdk")
UBER_JAR_NAME_PREFIX = "sagemaker-feature-store-spark-sdk"
SUPPORTED_SPARK_VERSIONS = ["3.1", "3.2", "3.3", "3.4", "3.5"]

# ── Centralised version map — single source of truth for all paths ──
SPARK_BUILD_VERSIONS = {
    "3.1": "3.1.3",
    "3.2": "3.2.4",
    "3.3": "3.3.4",
    "3.4": "3.4.3",
    "3.5": "3.5.1",
}

in_spark_sdk = os.path.isfile(SCALA_SPARK_DIR / "build.sbt")
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def read_version():
    return read(VERSION_PATH).strip()


def detect_pyspark_major_minor():
    """Detect installed PySpark major.minor version. Returns None if not installed."""
    try:
        import pyspark
        parts = pyspark.__version__.split(".")
        return f"{parts[0]}.{parts[1]}"
    except ImportError:
        return None


class CustomInstall(install):
    def run(self):
        install.run(self)
        spark_home_dir = os.environ.get("SPARK_HOME", None)

        if not spark_home_dir:
            print(
                "SPARK_HOME is not set. JARs are bundled in the package "
                "and will be selected automatically at runtime."
            )
            print("Installation finished.")
            return

        spark_version = detect_pyspark_major_minor()

        if spark_version is None:
            print(
                "WARNING: PySpark is not installed. Cannot determine which "
                "JAR to copy to SPARK_HOME. The correct JAR will be "
                "selected at runtime when PySpark is available."
            )
            print("Installation finished.")
            return

        if spark_version not in SUPPORTED_SPARK_VERSIONS:
            print(
                f"WARNING: Detected PySpark {spark_version} which is not "
                f"supported. Supported versions: {SUPPORTED_SPARK_VERSIONS}. "
                f"Skipping SPARK_HOME JAR copy."
            )
            print("Installation finished.")
            return

        # Find the matching JAR from the bundled JARs
        source_jar_name = f"{UBER_JAR_NAME_PREFIX}-{spark_version}.jar"
        jars_dir = Path(os.getcwd()) / Path(JARS_TARGET)
        source_jar_path = jars_dir / source_jar_name

        if not source_jar_path.exists():
            available = [
                f for f in os.listdir(jars_dir)
                if f.startswith(UBER_JAR_NAME_PREFIX) and f.endswith(".jar")
            ]
            print(
                f"WARNING: JAR for Spark {spark_version} not found at "
                f"{source_jar_path}. Available JARs: {available}. "
                f"Skipping SPARK_HOME JAR copy."
            )
            print("Installation finished.")
            return

        # Copy the single matching JAR to SPARK_HOME/jars with a generic
        # name so it replaces any previously installed version
        target_path = Path(spark_home_dir) / "jars" / f"{UBER_JAR_NAME_PREFIX}.jar"
        print(
            f"Detected PySpark {spark_version}. "
            f"Copying {source_jar_name} to {target_path}"
        )
        shutil.copy(source_jar_path, target_path)

        print("Installation finished.")


print("Starting the installation of SageMaker FeatureStore pyspark...")
if in_spark_sdk:
    shutil.copyfile(os.path.join("..", VERSION_PATH), VERSION_PATH)

    os.makedirs(JARS_TARGET, exist_ok=True)

    # ── Allow CI to build only one Spark version via env var ──────────
    # When SPARK_BUILD_VERSION is set (e.g. "3.5"), only that version is
    # built. This avoids building all 4 JARs in every CI matrix cell.
    # When unset (release builds), all versions are built.
    single_build = os.environ.get("SPARK_BUILD_VERSION", None)
    if single_build:
        if single_build not in SPARK_BUILD_VERSIONS:
            print(
                f"ERROR: SPARK_BUILD_VERSION={single_build} is not in "
                f"{list(SPARK_BUILD_VERSIONS.keys())}",
                file=sys.stderr,
            )
            exit(-1)
        versions_to_build = {single_build: SPARK_BUILD_VERSIONS[single_build]}
    else:
        versions_to_build = SPARK_BUILD_VERSIONS

    assembly_output_dir = SCALA_SPARK_DIR / "assembly-output"

    for major_minor, patch_version in versions_to_build.items():
        print(f"Building JAR for Spark {patch_version}...")

        # ── Clean assembly-output before each build to avoid stale JARs ──
        if assembly_output_dir.exists():
            shutil.rmtree(assembly_output_dir)

        p = subprocess.Popen(
            ["sbt", f"-DSPARK_VERSION={patch_version}", "assembly"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=SCALA_SPARK_DIR,
        )
        stdout, stderr = p.communicate()

        if p.returncode != 0:
            print(f"Failed to build JAR for Spark {patch_version}:")
            print(stderr.decode())
            exit(-1)

        # Retrieve the freshly built JAR
        if not assembly_output_dir.exists():
            print(
                f"assembly-output directory not found after building "
                f"Spark {patch_version}",
                file=sys.stderr,
            )
            exit(-1)

        built_jars = [
            assembly_output_dir / f
            for f in os.listdir(assembly_output_dir)
            if f.endswith(".jar")
        ]

        if len(built_jars) == 0:
            print(
                f"Failed to retrieve the jar classpath. "
                f"Can't package {patch_version}",
                file=sys.stderr,
            )
            exit(-1)

        if len(built_jars) > 1:
            print(
                f"WARNING: Multiple JARs found after clean build for "
                f"Spark {patch_version}: {built_jars}. Using first."
            )

        uber_jar_path = built_jars[0]

        target_jar_name = f"{UBER_JAR_NAME_PREFIX}-{major_minor}.jar"
        target_path = os.path.join(JARS_TARGET, target_jar_name)
        shutil.copy(uber_jar_path, target_path)
        print(f"Built: {target_path}")

else:
    if not os.path.exists(JARS_TARGET):
        print(
            "You need to be in the sagemaker-feature-store-spark root "
            "folder to package",
            file=sys.stderr,
        )
        exit(-1)

setup(
    name="sagemaker_feature_store_pyspark",
    author="Amazon Web Services",

    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="ML Amazon AWS AI FeatureStore SageMaker",

    version=read_version(),
    description="Amazon SageMaker FeatureStore PySpark Bindings",
    license="Apache License 2.0",
    zip_safe=False,

    packages=[
        "feature_store_pyspark",
        "feature_store_pyspark.jars",
    ],

    package_dir={
        "feature_store_pyspark": "src/feature_store_pyspark",
        "feature_store_pyspark.jars": "deps/jars",
    },

    include_package_data=True,

    scripts=["bin/feature-store-pyspark-dependency-jars"],

    package_data={
        "feature_store_pyspark.jars": ["*.jar"],
    },

    python_requires=">=3.8",

    install_requires=[],

    cmdclass={
        "install": CustomInstall,
    },
)
