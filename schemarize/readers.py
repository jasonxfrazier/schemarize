# schemarize/readers.py
import json
import gzip
import bz2
import ijson
from typing import Union, Iterator, Dict, Any, TextIO, Optional
from io import BytesIO, TextIOBase
import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def read_jsonl(source: Union[str, TextIO]) -> Iterator[Dict[str, Any]]:
    """
    Read a JSON Lines (JSONL) file or file-like object and yield one record (dict) at a time.
    Supports plain text, .gz, and .bz2 files. Raises JSONDecodeError on invalid JSON.
    """
    if isinstance(source, str):
        if source.endswith(".gz"):
            file_obj = gzip.open(source, "rt", encoding="utf-8")
        elif source.endswith(".bz2"):
            file_obj = bz2.open(source, "rt", encoding="utf-8")
        else:
            file_obj = open(source, "rt", encoding="utf-8")
        close_obj = True
    else:
        if isinstance(source, TextIOBase):
            raw = source.read()
            file_obj = BytesIO(raw.encode("utf-8"))
            close_obj = True
        else:
            file_obj = source  # type: ignore
            close_obj = False

    try:
        for idx, raw_line in enumerate(file_obj, start=1):
            line = raw_line
            if isinstance(line, memoryview):
                line = line.tobytes()
            if isinstance(line, (bytes, bytearray)):
                line = line.decode("utf-8")
            if not isinstance(line, str):
                continue
            text = line.strip()
            if not text:
                continue
            try:
                yield json.loads(text)
            except json.JSONDecodeError as err:
                raise json.JSONDecodeError(
                    f"Error parsing JSON on line {idx}: {err.msg}",
                    err.doc,
                    err.pos
                )
    finally:
        if close_obj:
            file_obj.close()


def read_json_array(source: Union[str, TextIO]) -> Iterator[Dict[str, Any]]:
    """
    Read a JSON array from a file or file-like object and yield each element as a dict.
    Supports plain JSON, .gz, and .bz2 files. Raises JSONDecodeError on errors.
    """
    if isinstance(source, str):
        if source.endswith(".gz"):
            file_obj = gzip.open(source, "rb")
        elif source.endswith(".bz2"):
            file_obj = bz2.open(source, "rb")
        else:
            file_obj = open(source, "rb")
        close_obj = True
    else:
        if isinstance(source, TextIOBase):
            raw = source.read()
            file_obj = BytesIO(raw.encode("utf-8"))
            close_obj = True
        else:
            file_obj = source  # type: ignore
            close_obj = False

    try:
        for item in ijson.items(file_obj, "item"):
            yield item
    except Exception as err:
        raise json.JSONDecodeError(f"Error parsing JSON array: {err}", "", 0)
    finally:
        if close_obj:
            file_obj.close()


def read_csv(
    source: Union[str, TextIO],
    delimiter: str = ",",
    encoding: str = "utf-8",
    chunk_size: Optional[int] = None
) -> Iterator[Dict[str, Any]]:
    """
    Read a CSV file and yield each row as a dict.
    Supports plain text, .gz, .bz2 files, and file-like objects.
    If chunk_size is None, streams via csv.DictReader;
    else uses pandas.read_csv with chunksize.
    """
    if isinstance(source, str):
        if source.endswith('.gz'):
            file_obj = gzip.open(source, 'rt', encoding=encoding)
        elif source.endswith('.bz2'):
            file_obj = bz2.open(source, 'rt', encoding=encoding)
        else:
            file_obj = open(source, 'rt', encoding=encoding)
        close_obj = True
    else:
        file_obj = source
        close_obj = False

    try:
        if chunk_size is None:
            reader = csv.DictReader(file_obj, delimiter=delimiter)
            for row in reader:
                if any(v is None for v in row.values()):
                    raise RuntimeError(f"Malformed CSV row at line {reader.line_num}")
                yield row
        else:
            df_iter = pd.read_csv(
                source if isinstance(source, str) else file_obj,
                delimiter=delimiter,
                encoding=encoding,
                chunksize=chunk_size
            )
            for chunk in df_iter:
                for rec in chunk.to_dict(orient="records"):
                    yield rec
    except Exception as err:
        raise RuntimeError(f"Error reading CSV: {err}") from err
    finally:
        if close_obj:
            file_obj.close()


def read_parquet(
    source: Union[str, TextIO],
    batch_size: Optional[int] = None
) -> Iterator[Dict[str, Any]]:
    """
    Read a Parquet file and yield each row as a dict.
    Uses PyArrow for streaming row batches.
    """
    if isinstance(source, str):
        try:
            dataset = pq.ParquetFile(source)
        except Exception as err:
            raise RuntimeError(f"Error opening Parquet file: {err}") from err
        iterator = dataset.iter_batches(batch_size=batch_size) if batch_size is not None else dataset.iter_batches()
        close_buffer = False
    else:
        raw = source.read() if hasattr(source, 'read') else b''
        buf = raw if isinstance(raw, (bytes, bytearray)) else raw.encode('utf-8')
        file_obj = BytesIO(buf)
        try:
            dataset = pq.ParquetFile(file_obj)
        except Exception as err:
            raise RuntimeError(f"Error opening Parquet buffer: {err}") from err
        iterator = dataset.iter_batches(batch_size=batch_size) if batch_size is not None else dataset.iter_batches()
        close_buffer = True

    try:
        for batch in iterator:
            for rec in batch.to_pylist():
                yield rec
    except Exception as err:
        raise RuntimeError(f"Error reading Parquet: {err}") from err
    finally:
        if close_buffer:
            file_obj.close()


def read_dataframe(
    source: Union[pd.DataFrame, pa.Table]
) -> Iterator[Dict[str, Any]]:
    """
    Read from a pandas DataFrame or PyArrow Table and yield each row as a dict.
    Raises RuntimeError on unsupported type.
    """
    try:
        if isinstance(source, pd.DataFrame):
            for rec in source.to_dict(orient="records"):
                yield rec # type: ignore
        elif isinstance(source, pa.Table):
            for batch in source.to_batches():
                for rec in batch.to_pylist():
                    yield rec
        else:
            raise RuntimeError(f"Unsupported type for read_dataframe: {type(source)}")
    except Exception as err:
        raise RuntimeError(f"Error reading DataFrame: {err}") from err