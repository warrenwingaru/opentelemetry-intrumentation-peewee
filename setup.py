import distutils.cmd
import json
import os
from configparser import ConfigParser

import setuptools

config = ConfigParser()
config.read("setup.cfg")

extras_require = {}
if "options.extras_require" in config:
    for key, value in config["options.extras_require"].items():
        extras_require[key] = [v for v in value.split("\n") if v.strip()]

BASE_DIR = os.path.dirname(__file__)
PACKAGE_INFO = {}

VERSION_FILENAME = os.path.join(
    BASE_DIR, "src", "opentelemetry", "instrumentation", "peewee", "version.py"
)

with open(VERSION_FILENAME, encoding="utf-8") as f:
    exec(f.read(), PACKAGE_INFO)

PACKAGE_FILENAME = os.path.join(
    BASE_DIR, "src", "opentelemetry", "instrumentation", "peewee", "package.py"
)

with open(PACKAGE_FILENAME, encoding="utf-8") as f:
    exec(f.read(), PACKAGE_INFO)

# Mark any instruments/runtime dependencies as test dependencies as well.
extras_require["instruments"] = PACKAGE_INFO["_instruments"]
test_deps = extras_require.get("test", [])
for deps in extras_require.get("instruments", []):
    test_deps.append(deps)

extras_require["test"] = test_deps

class JSONMetadataCommand(distutils.cmd.Command):

    description = (
        "print out package metadata as JSON. This is used by OpenTelemetry dev scripts to  ",
        "auto-generate code in other places"
    )
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        metadata = {
            "name": config['metadata']['name'],
            'version': PACKAGE_INFO["__version__"],
            'instruments': PACKAGE_INFO["_instruments"],
        }
        print(json.dumps(metadata))

print(extras_require)
setuptools.setup(
    cmdclass={"meta": JSONMetadataCommand},
    version=PACKAGE_INFO["__version__"],
    extras_require=extras_require,
)