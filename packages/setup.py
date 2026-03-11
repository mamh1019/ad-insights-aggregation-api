from setuptools import setup, find_packages

import os

package_directory = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "bootstrap")
)
project_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)

init_text = """
import os
import sys

pythonpath = "./modules:./tasks"

if pythonpath:
    for path in pythonpath.split(':'):
        if path not in sys.path:
            sys.path.append(os.path.join('{}', path))

import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
""".format(
    project_directory
)

with open(
    os.path.join(package_directory, "core.py"), "w", encoding="utf-8"
) as init_file:
    init_file.write(init_text)

setup(name="bootstrap", version="1.0.0", packages=find_packages())
