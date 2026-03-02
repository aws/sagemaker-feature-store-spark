# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

def classpath_jars():
    """Returns a list with the paths to the required jar files.
    The sagemakerpyspark library is mostly a wrapper of the scala sagemakerspark sdk and it
    depends on a set of jar files to work correctly. This function retrieves the location
    of these jars in the local installation.
    Returns:
        List of absolute paths.
    """
    import pkg_resources
    import os
    
    try:
        import pyspark
    except ImportError:
        raise ImportError(
            "PySpark is required. Install it with: pip install pyspark"
        )

    pkg_dir = __name__

    jars_dir = "/jars/"
    os.environ['PYTHON_EGG_CACHE'] = pkg_dir + '/tmp'
    
    bundled_jars = pkg_resources.resource_listdir(pkg_dir, jars_dir)
    
    # Check current pyspark version and filter jars
    spark_version = pyspark.__version__
    sv_parts = spark_version.split(".")
    major_minor = f"{sv_parts[0]}.{sv_parts[1]}"
    target_jar = f"sagemaker-feature-store-spark-sdk-{major_minor}.jar"
    
    if target_jar not in bundled_jars:
        raise RuntimeError(
            f"No JAR found for Spark {major_minor}. "
            f"Available: {bundled_jars}. "
            f"Supported Spark versions: 3.2, 3.3, 3.4, 3.5"
        )
            
    jars = [pkg_resources.resource_filename(pkg_dir, jars_dir + target_jar)]

    return jars


__all__ = ['FeatureStoreManager', 'classpath_jars', 'wrapper']
