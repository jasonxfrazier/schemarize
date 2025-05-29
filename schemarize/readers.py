# schemarize/readers.py
import json
import gzip
import bz2
import ijson
from typing import Union, Iterator, Dict, Any, TextIO
from io import BytesIO, TextIOBase
import pandas as pd


def read_jsonl(source: Union[str, TextIO]) -> Iterator[Dict[str, Any]]:
    """
    Read a JSON Lines (JSONL) file or file-like object and yield one record (dict) at a time.
    Supports plain text, .gz, and .bz2 files. Raises JSONDecodeError on invalid JSON.
    """
    if isinstance(source, str):
        if source.endswith(".gz"):
            file = gzip.open(source, "rt", encoding="utf-8")
        elif source.endswith(".bz2"):
            file = bz2.open(source, "rt", encoding="utf-8")
        else:
            file = open(source, "rt", encoding="utf-8")
        should_close = True
    else:
        if isinstance(source, TextIOBase):
            content = source.read()
            file = BytesIO(content.encode('utf-8'))
            should_close = True
        else:
            file = source  # type: ignore
            should_close = False

    try:
        for idx, raw_line in enumerate(file, start=1):
            # Normalize binary data to str
            line_data = raw_line
            if isinstance(line_data, memoryview):
                line_data = line_data.tobytes()
            if isinstance(line_data, (bytes, bytearray)):
                line_data = line_data.decode('utf-8')
            # Skip non-string lines
            if not isinstance(line_data, str):
                continue
            text = line_data.strip()
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
        if should_close:
            file.close()


def read_json_array(source: Union[str, TextIO]) -> Iterator[Dict[str, Any]]:
    """
    Read a JSON array from a file or file-like object and yield each element as a dict.
    Supports plain JSON, .gz, and .bz2 files via streaming parsing to avoid full memory load.
    Raises JSONDecodeError on invalid JSON.
    """
    if isinstance(source, str):
        if source.endswith(".gz"):
            file = gzip.open(source, "rb")
        elif source.endswith(".bz2"):
            file = bz2.open(source, "rb")
        else:
            file = open(source, "rb")
        should_close = True
    else:
        if isinstance(source, TextIOBase):
            content = source.read()
            file = BytesIO(content.encode('utf-8'))
            should_close = True
        else:
            file = source  # type: ignore
            should_close = False

    try:
        for item in ijson.items(file, 'item'):
            yield item
    except Exception as err:
        raise json.JSONDecodeError(
            f"Error parsing JSON array: {err}",
            '',
            0
        )
    finally:
        if should_close:
            file.close()

def read_csv(source: Union[str, TextIO], delimiter: str = ',', encoding: str = 'utf-8', chunk_size: int | None = None) -> Iterator[Dict[str, Any]]:
    '''
    Read a CSV file and yield each row as a dict.
    Supports plain text, .gz, .bz2 files, and file-like objects.
    If chunk_size is None, uses csv module for streaming.
    If chunk_size is provided, uses pandas.read_csv with chunksize.
    '''
    import csv

    # handle file opening for paths or use provided file-like
    if isinstance(source, str):
        if source.endswith('.gz'):
            f = gzip.open(source, 'rt', encoding=encoding)
        elif source.endswith('.bz2'):
            f = bz2.open(source, 'rt', encoding=encoding)
        else:
            f = open(source, 'rt', encoding=encoding)
        own_file = True
    else:
        f = source
        own_file = False

    try:
        if chunk_size is None:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                yield row
        else:
            df_iter = pd.read_csv(source if isinstance(source, str) else f,
                                  delimiter=delimiter,
                                  encoding=encoding,
                                  chunksize=chunk_size)
            for df_chunk in df_iter:
                for record in df_chunk.to_dict(orient='records'):
                    yield record
    finally:
        if own_file:
            f.close()
