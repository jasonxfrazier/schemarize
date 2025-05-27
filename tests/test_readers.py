import io
import gzip
import bz2
import json
import pytest
from pathlib import Path
from typing import Any
from json import JSONDecodeError

from schemarize.readers import read_jsonl

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
    # empty file yields nothing
    path: Path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    assert list(read_jsonl(str(path))) == []

def test_read_jsonl_invalid_json(tmp_path: Path) -> None:
    """
    A file with invalid JSON should raise JSONDecodeError when reading.
    """
    path: Path = tmp_path / "invalid.jsonl"
    path.write_text("not a json line\n", encoding="utf-8")
    with pytest.raises(JSONDecodeError):
        list(read_jsonl(str(path)))