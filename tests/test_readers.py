# tests/test_readers.py
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

from schemarize.readers import (
    read_jsonl,
    read_json_array,
    read_csv,
    read_parquet,
    read_dataframe
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


def test_read_jsonl_valid(tmp_path: Path) -> None:
    lines = [{"a": 1}, {"b": 2}]
    path = create_jsonl_file(tmp_path, lines)
    assert list(read_jsonl(str(path))) == lines


def test_read_jsonl_invalid(tmp_path: Path) -> None:
    path = tmp_path / "invalid.jsonl"
    path.write_text("not a json line\n", encoding="utf-8")
    with pytest.raises(JSONDecodeError):
        list(read_jsonl(str(path)))


def test_read_json_array_valid(tmp_path: Path) -> None:
    arr = [{"x": 1}, {"y": 2}]
    path = create_json_array_file(tmp_path, arr)
    assert list(read_json_array(str(path))) == arr


def test_read_json_array_invalid(tmp_path: Path) -> None:
    path = tmp_path / "invalid.json"
    path.write_text("[invalid]", encoding="utf-8")
    with pytest.raises(JSONDecodeError):
        list(read_json_array(str(path)))


def test_read_csv_valid(tmp_path: Path) -> None:
    headers = ["a", "b"]
    rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
    path = create_csv_file(tmp_path, rows, headers)
    assert list(read_csv(str(path))) == rows


def test_read_csv_invalid(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("a,b\n1,2\nbadrow", encoding="utf-8")
    with pytest.raises(RuntimeError):
        list(read_csv(str(path)))


def test_read_parquet_valid(tmp_path: Path) -> None:
    df = pd.DataFrame([{"p": 5}, {"p": 6}])
    path = tmp_path / "data.parquet"
    df.to_parquet(path)
    assert list(read_parquet(str(path))) == df.to_dict(orient='records')


def test_read_parquet_invalid(tmp_path: Path) -> None:
    path = tmp_path / "bad.parquet"
    path.write_text("not parquet", encoding="utf-8")
    with pytest.raises(RuntimeError):
        list(read_parquet(str(path)))


def test_read_dataframe_pandas() -> None:
    df = pd.DataFrame([{"m": 7}, {"m": 8}])
    assert list(read_dataframe(df)) == df.to_dict(orient='records')


def test_read_dataframe_pyarrow() -> None:
    df = pd.DataFrame([{"n": 9}, {"n": 10}])
    table = pa.Table.from_pandas(df)
    assert list(read_dataframe(table)) == df.to_dict(orient='records')


def test_read_dataframe_invalid():
    with pytest.raises(RuntimeError):
        list(read_dataframe("not supported"))
