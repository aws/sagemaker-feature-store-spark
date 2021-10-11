# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    import os
    import atexit
    from importlib import resources
    from contextlib import ExitStack

    # Control the life cycle of temporary files and manage explicitely
    # instead of cleaning them up at once
    file_manager = ExitStack()
    atexit.register(file_manager.close)
    pkg_dir = __name__

    jars_dir = 'jars'
    os.environ['PYTHON_EGG_CACHE'] = pkg_dir + '/tmp'

    package_dir = resources.files(__name__).joinpath(jars_dir)
    bundled_jars = package_dir.iterdir()

    jars = []
    for jar in bundled_jars:
        with jar as p:
            jars.append(str(p))
            file_manager.enter_context(p)

    return jars


__all__ = ['FeatureStoreManager', 'classpath_jars']
