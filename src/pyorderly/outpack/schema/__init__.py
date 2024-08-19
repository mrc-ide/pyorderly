import json
import os
import os.path

import importlib_resources
from jsonschema import Draft7Validator
from referencing import Registry, Resource


def validate(instance, schema_name):
    root = os.path.dirname(schema_name)

    def retrieve(path):
        return retrieve_from_filesystem(os.path.join(root, path))

    schema = json.loads(read_schema(schema_name))
    validator = Draft7Validator(schema, registry=Registry(retrieve=retrieve))
    validator.validate(instance)


def retrieve_from_filesystem(path: str):
    contents = json.loads(read_schema(path))
    return Resource.from_contents(contents)


def outpack_schema_version():
    data = read_schema("outpack/config.json")
    return json.loads(data)["version"]


def read_schema(name):
    schema = importlib_resources.files("pyorderly.outpack.schema").joinpath(
        name
    )
    return schema.read_text()
