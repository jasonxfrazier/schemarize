import io
import gzip
import bz2
import json
import pytest
from pathlib import Path
from typing import Any, Dict
from json import JSONDecodeError

import pandas as pd
import pyarrow as pa
import math

def normalize_nans(obj):
    """Recursively replace all float('nan') with None in dicts/lists."""
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: normalize_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_nans(x) for x in obj]
    return obj

from schemarize.readers import (
    read_jsonl,
    read_json_array,
    read_csv,
    read_parquet,
    read_dataframe,
    infer_schema,  # <-- Don't forget this import
)

def create_jsonl_file(tmp_path: Path, lines: list[Dict[str, Any]], suffix: str = "") -> Path:
    filename = "sample.jsonl" + suffix
    path = tmp_path / filename
    content = "\n".join(json.dumps(line) for line in lines) + "\n"
    if suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(content)
    elif suffix == ".bz2":
        with bz2.open(path, "wb") as f:
            f.write(content.encode("utf-8"))
    else:
        path.write_text(content, encoding="utf-8")
    return path

def create_json_array_file(tmp_path: Path, arr: list[Dict[str, Any]], suffix: str = "") -> Path:
    filename = "sample.json" + suffix
    path = tmp_path / filename
    content = json.dumps(arr)
    if suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(content)
    elif suffix == ".bz2":
        with bz2.open(path, "wb") as f:
            f.write(content.encode("utf-8"))
    else:
        path.write_text(content, encoding="utf-8")
    return path

def create_csv_file(tmp_path: Path, rows: list[Dict[str, Any]], headers: list[str], suffix: str = "") -> Path:
    filename = "sample.csv" + suffix
    path = tmp_path / filename
    content = ",".join(headers) + "\n"
    for row in rows:
        content += ",".join(str(row.get(h, "")) for h in headers) + "\n"
    if suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(content)
    elif suffix == ".bz2":
        with bz2.open(path, "wb") as f:
            f.write(content.encode("utf-8"))
    else:
        path.write_text(content, encoding="utf-8")
    return path

def test_read_jsonl_valid(tmp_path: Path):
    lines = [{"a": 1}, {"b": 2}, {}]
    path = create_jsonl_file(tmp_path, lines)
    assert list(read_jsonl(str(path))) == lines

def test_read_jsonl_invalid(tmp_path: Path):
    path = tmp_path / "invalid.jsonl"
    path.write_text("not a json line\n", encoding="utf-8")
    with pytest.raises(JSONDecodeError):
        list(read_jsonl(str(path)))

def test_read_json_array_valid(tmp_path: Path):
    arr = [{"p": 1}, {"q": 2}]
    path = create_json_array_file(tmp_path, arr)
    assert list(read_json_array(str(path))) == arr

def test_read_json_array_invalid(tmp_path: Path):
    path = tmp_path / "invalid.json"
    path.write_text("[not valid json]", encoding="utf-8")
    with pytest.raises(JSONDecodeError):
        list(read_json_array(str(path)))

def test_read_csv_plain(tmp_path: Path):
    rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
    headers = ["a", "b"]
    path = create_csv_file(tmp_path, rows, headers)
    assert list(read_csv(str(path))) == rows

def test_read_csv_invalid(tmp_path: Path):
    path = tmp_path / "invalid.csv"
    content = "col1,col2\nvalue1,value2\nbadrowwithoutcomma"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(RuntimeError):
        list(read_csv(str(path)))

def test_read_parquet_valid(tmp_path: Path):
    df = pd.DataFrame([{"x": 10, "y": "foo"}, {"x": 20, "y": "bar"}])
    path = tmp_path / "sample.parquet"
    df.to_parquet(path)
    expected = df.to_dict(orient="records")
    assert list(read_parquet(str(path))) == expected

def test_read_parquet_with_batch_size(tmp_path: Path):
    df = pd.DataFrame([{"a": 10}, {"a": 20}, {"a": 30}])
    path = tmp_path / "batch.parquet"
    df.to_parquet(path)
    result = list(read_parquet(str(path), batch_size=2))
    assert result == df.to_dict(orient="records")

def test_read_parquet_invalid(tmp_path: Path):
    path = tmp_path / "invalid.parquet"
    path.write_text("not a parquet file", encoding="utf-8")
    with pytest.raises(RuntimeError):
        list(read_parquet(str(path)))

def test_read_dataframe_pandas():
    df = pd.DataFrame([{"x": 1}, {"y": 2}])
    assert normalize_nans(list(read_dataframe(df))) == normalize_nans(df.to_dict(orient="records"))

def test_read_dataframe_pyarrow():
    df = pd.DataFrame([{"x": 3}, {"y": 4}])
    table = pa.Table.from_pandas(df)
    assert normalize_nans(list(read_dataframe(table))) == normalize_nans(df.to_dict(orient="records"))


def test_read_dataframe_invalid():
    with pytest.raises(RuntimeError):
        list(read_dataframe("not a table or dataframe"))

def test_infer_schema_jsonl(tmp_path: Path):
    lines = [{"a": 1}, {"b": 2}, {}]
    path = create_jsonl_file(tmp_path, lines)
    assert infer_schema(str(path)) == lines

def test_infer_schema_sample_size(tmp_path: Path):
    lines = [{"x": 10}, {"x": 20}, {"x": 30}]
    path = create_jsonl_file(tmp_path, lines)
    assert infer_schema(str(path), sample_size=2) == lines[:2]

def test_infer_schema_dataframe():
    df = pd.DataFrame([{"alpha": 1}, {"beta": 2}])
    assert normalize_nans(list(read_dataframe(df))) == normalize_nans(df.to_dict(orient="records"))
