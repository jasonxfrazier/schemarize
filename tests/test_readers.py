# tests/test_readers.py
import io
import gzip
import bz2
import json
import pytest
from pathlib import Path
from typing import Any
from json import JSONDecodeError

from schemarize.readers import read_jsonl, read_json_array


def create_jsonl_file(
    tmp_path: Path,
    lines: list[dict[str, Any]],
    suffix: str = ""
) -> Path:
    """
    Create a JSONL test file, optionally compressed, and return its Path.
    """
    filename: str = "sample.jsonl" + suffix
    path: Path = tmp_path / filename
    content: str = "\n".join(json.dumps(line) for line in lines) + "\n"
    
    if suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(content)
    elif suffix == ".bz2":
        with bz2.open(path, "wb") as f:
            f.write(content.encode("utf-8"))
    else:
        path.write_text(content, encoding="utf-8")
    
    return path


def create_json_array_file(
    tmp_path: Path,
    arr: list[dict[str, Any]],
    suffix: str = ""
) -> Path:
    """
    Create a JSON array file, optionally compressed, and return its Path.
    """
    filename: str = "sample.json" + suffix
    path: Path = tmp_path / filename
    content: str = json.dumps(arr)
    
    if suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(content)
    elif suffix == ".bz2":
        with bz2.open(path, "wb") as f:
            f.write(content.encode("utf-8"))
    else:
        path.write_text(content, encoding="utf-8")
    
    return path


def test_read_jsonl_plain(tmp_path: Path) -> None:
    lines: list[dict[str, Any]] = [{"a": 1}, {"b": 2}, {}]
    path: Path = create_jsonl_file(tmp_path, lines)
    assert list(read_jsonl(str(path))) == lines


def test_read_jsonl_gzip(tmp_path: Path) -> None:
    lines: list[dict[str, Any]] = [{"x": "x"}, {"y": "y"}]
    path: Path = create_jsonl_file(tmp_path, lines, suffix=".gz")
    assert list(read_jsonl(str(path))) == lines


def test_read_jsonl_bz2(tmp_path: Path) -> None:
    lines: list[dict[str, Any]] = [{"foo": "bar"}]
    path: Path = create_jsonl_file(tmp_path, lines, suffix=".bz2")
    assert list(read_jsonl(str(path))) == lines


def test_read_jsonl_file_like() -> None:
    lines: list[dict[str, Any]] = [{"k": "v"}]
    content: str = "\n".join(json.dumps(line) for line in lines) + "\n"
    fake: io.StringIO = io.StringIO(content)
    assert list(read_jsonl(fake)) == lines


def test_read_jsonl_empty(tmp_path: Path) -> None:
    path: Path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    assert list(read_jsonl(str(path))) == []


def test_read_jsonl_invalid_json(tmp_path: Path) -> None:
    path: Path = tmp_path / "invalid.jsonl"
    path.write_text("not a json line\n", encoding="utf-8")
    with pytest.raises(JSONDecodeError):
        list(read_jsonl(str(path)))


def test_read_json_array_plain(tmp_path: Path) -> None:
    arr: list[dict[str, Any]] = [{"p": 1}, {"q": 2}]
    path: Path = create_json_array_file(tmp_path, arr)
    assert list(read_json_array(str(path))) == arr


def test_read_json_array_gzip(tmp_path: Path) -> None:
    arr: list[dict[str, Any]] = [{"m": "n"}, {"o": "p"}]
    path: Path = create_json_array_file(tmp_path, arr, suffix=".gz")
    assert list(read_json_array(str(path))) == arr


def test_read_json_array_bz2(tmp_path: Path) -> None:
    arr: list[dict[str, Any]] = [{"x": True}, {"y": False}]
    path: Path = create_json_array_file(tmp_path, arr, suffix=".bz2")
    assert list(read_json_array(str(path))) == arr


def test_read_json_array_file_like() -> None:
    arr: list[dict[str, Any]] = [{"foo": 123}, {"bar": 456}]
    content: str = json.dumps(arr)
    fake: io.StringIO = io.StringIO(content)
    assert list(read_json_array(fake)) == arr


def test_read_json_array_invalid_json(tmp_path: Path) -> None:
    # malformed JSON array should raise JSONDecodeError
    path: Path = tmp_path / "invalid_array.json"
    path.write_text("[not valid json]", encoding="utf-8")
    with pytest.raises(JSONDecodeError):
        list(read_json_array(str(path)))