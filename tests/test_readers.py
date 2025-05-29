# tests/test_csv_reader.py
import io
import gzip
import bz2
import pytest
from pathlib import Path
from typing import Any, Dict

from schemarize.readers import read_csv

def create_csv_file(
    tmp_path: Path,
    rows: list[Dict[str, Any]],
    headers: list[str],
    suffix: str = ""
) -> Path:
    """
    Create a CSV test file, optionally compressed, and return its Path.
    """
    filename = "sample.csv" + suffix
    path = tmp_path / filename
    # build CSV content
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


def test_read_csv_plain(tmp_path: Path) -> None:
    headers = ["a", "b"]
    rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
    path = create_csv_file(tmp_path, rows, headers)
    assert list(read_csv(str(path))) == rows


def test_read_csv_gzip(tmp_path: Path) -> None:
    headers = ["x", "y"]
    rows = [{"x": "5", "y": "6"}]
    path = create_csv_file(tmp_path, rows, headers, suffix=".gz")
    assert list(read_csv(str(path))) == rows


def test_read_csv_bz2(tmp_path: Path) -> None:
    headers = ["m", "n"]
    rows = [{"m": "true", "n": "false"}]
    path = create_csv_file(tmp_path, rows, headers, suffix=".bz2")
    assert list(read_csv(str(path))) == rows


def test_read_csv_file_like() -> None:
    headers = ["p", "q"]
    rows = [{"p": "alpha", "q": "beta"}]
    content = "p,q\nalpha,beta\n"
    fake = io.StringIO(content)
    assert list(read_csv(fake)) == rows


def test_read_csv_chunk_size(tmp_path: Path) -> None:
    headers = ["k", "v"]
    rows = [{"k": "A", "v": "B"}, {"k": "C", "v": "D"}]
    path = create_csv_file(tmp_path, rows, headers)
    assert list(read_csv(str(path), chunk_size=1)) == rows
