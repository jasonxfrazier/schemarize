import json
from typing import Any

import pytest
import yaml

from schemarize.schema import Schema

# Examples for testing
FLAT_SCHEMA: Any = {"a": "int", "b": "str", "c": "float"}
NESTED_SCHEMA: Any = {
    "user": {"id": "int", "profile": {"name": "str", "tags": ["str"]}},
    "active": "bool",
}
EMPTY_SCHEMA: Any = {}


@pytest.fixture
def tmp_file(tmp_path):
    def _tmp_file(name):
        return tmp_path / name

    return _tmp_file


def test_to_dict_flat():
    schema = Schema(FLAT_SCHEMA)
    assert schema.to_dict() == FLAT_SCHEMA


def test_to_dict_nested():
    schema = Schema(NESTED_SCHEMA)
    assert schema.to_dict() == NESTED_SCHEMA


def test_to_json_and_pretty():
    schema = Schema(NESTED_SCHEMA)
    json_str = schema.to_json()
    json_str_pretty = schema.to_json(pretty=True)
    assert json.loads(json_str) == NESTED_SCHEMA
    assert json.loads(json_str_pretty) == NESTED_SCHEMA
    assert json_str_pretty.startswith("{\n")


def test_to_yaml_roundtrip():
    schema = Schema(NESTED_SCHEMA)
    yaml_str = schema.to_yaml()
    loaded = yaml.safe_load(yaml_str)
    assert loaded == NESTED_SCHEMA


def test_to_csv_flat():
    schema = Schema(FLAT_SCHEMA)
    csv_str = schema.to_csv()
    assert "a" in csv_str and "b" in csv_str and "c" in csv_str
    assert "int" in csv_str and "str" in csv_str and "float" in csv_str


def test_to_csv_nested():
    schema = Schema(NESTED_SCHEMA)
    csv_str = schema.to_csv()
    # With nested, the json_normalize flattens keys
    assert "user.profile.name" in csv_str or "user.profile.tags" in csv_str


def test_to_dict_empty():
    schema = Schema(EMPTY_SCHEMA)
    assert schema.to_dict() == {}


def test_to_json_empty():
    schema = Schema(EMPTY_SCHEMA)
    assert schema.to_json() == "{}"


def test_to_yaml_empty():
    schema = Schema(EMPTY_SCHEMA)
    assert schema.to_yaml().strip() == "{}"


def test_to_csv_empty():
    schema = Schema(EMPTY_SCHEMA)
    assert "Unnamed: 0" in schema.to_csv() or schema.to_csv().strip() == ""


def test_save_json(tmp_path):
    schema = Schema(FLAT_SCHEMA)
    out = tmp_path / "s.json"
    schema.save(str(out))
    assert out.exists()
    with open(out, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded == FLAT_SCHEMA


def test_save_yaml(tmp_path):
    schema = Schema(NESTED_SCHEMA)
    out = tmp_path / "s.yaml"
    schema.save(str(out))
    assert out.exists()
    with open(out, "r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f)
    assert loaded == NESTED_SCHEMA


def test_save_csv(tmp_path):
    schema = Schema(FLAT_SCHEMA)
    out = tmp_path / "s.csv"
    schema.save(str(out))
    assert out.exists()
    with open(out, "r", encoding="utf-8") as f:
        content = f.read()
    assert "a" in content and "int" in content


def test_save_to_explicit_format(tmp_path):
    schema = Schema(FLAT_SCHEMA)
    # Save to .txt as csv
    out = tmp_path / "schema.txt"
    schema.save(str(out), format="csv")
    with open(out, "r", encoding="utf-8") as f:
        assert "a" in f.read()


def test_save_unsupported_format(tmp_path):
    schema = Schema(FLAT_SCHEMA)
    out = tmp_path / "s.unsupported"
    with pytest.raises(ValueError):
        schema.save(str(out))


def test_repr():
    schema = Schema(FLAT_SCHEMA)
    out = repr(schema)
    assert "Schema" in out and "a" in out
