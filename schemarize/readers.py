import json
import gzip
import bz2
import ijson
from typing import Union, Iterator, Dict, Any, TextIO, Optional
from io import BytesIO, TextIOBase
import csv
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


def read_jsonl(source: Union[str, TextIO]) -> Iterator[Dict[str, Any]]:
    """
    Read a JSON Lines (JSONL) file or file-like object and yield one record (dict) at a time.
    Supports plain text, .gz, and .bz2 files. Raises JSONDecodeError on invalid JSON.
    """
    if isinstance(source, str):
        if source.endswith('.gz'):
            file = gzip.open(source, 'rt', encoding='utf-8')
        elif source.endswith('.bz2'):
            file = bz2.open(source, 'rt', encoding='utf-8')
        else:
            file = open(source, 'rt', encoding='utf-8')
        should_close = True
    else:
        if isinstance(source, TextIOBase):
            raw = source.read()
            file = BytesIO(raw.encode('utf-8'))
            should_close = True
        else:
            file = source  # type: ignore
            should_close = False

    try:
        for idx, raw_line in enumerate(file, start=1):
            line_data = raw_line
            if isinstance(line_data, memoryview):
                line_data = line_data.tobytes()
            if isinstance(line_data, (bytes, bytearray)):
                line_data = line_data.decode('utf-8')
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
    Supports plain JSON, .gz, and .bz2 files via streaming parsing. Raises JSONDecodeError on errors.
    """
    if isinstance(source, str):
        if source.endswith('.gz'):
            file = gzip.open(source, 'rb')
        elif source.endswith('.bz2'):
            file = bz2.open(source, 'rb')
        else:
            file = open(source, 'rb')
        should_close = True
    else:
        if isinstance(source, TextIOBase):
            raw = source.read()
            file = BytesIO(raw.encode('utf-8'))
            should_close = True
        else:
            file = source  # type: ignore
            should_close = False

    try:
        for item in ijson.items(file, 'item'):
            yield item
    except Exception as err:
        raise json.JSONDecodeError(f"Error parsing JSON array: {err}", '', 0)
    finally:
        if should_close:
            file.close()


def read_csv(
    source: Union[str, TextIO],
    delimiter: str = ',',
    encoding: str = 'utf-8',
    chunk_size: Optional[int] = None
) -> Iterator[Dict[str, Any]]:
    '''
    Read a CSV file and yield each row as a dict.
    Supports plain text, .gz, .bz2 files, and file-like objects.
    If chunk_size is None, streams via csv.DictReader;
    else uses pandas.read_csv with chunksize.
    '''
    if isinstance(source, str):
        if source.endswith('.gz'):
            file = gzip.open(source, 'rt', encoding=encoding)
        elif source.endswith('.bz2'):
            file = bz2.open(source, 'rt', encoding=encoding)
        else:
            file = open(source, 'rt', encoding=encoding)
        should_close = True
    else:
        file = source
        should_close = False

    try:
        if chunk_size is None:
            reader = csv.DictReader(file, delimiter=delimiter)
            for row in reader:
                if any(v is None for v in row.values()):
                    raise RuntimeError(f"Malformed CSV row at line {reader.line_num}")
                yield row
        else:
            df_iter = pd.read_csv(
                source if isinstance(source, str) else file,
                delimiter=delimiter,
                encoding=encoding,
                chunksize=chunk_size
            )
            for df_chunk in df_iter:
                for record in df_chunk.to_dict(orient='records'):
                    yield record
    except Exception as err:
        raise RuntimeError(f"Error reading CSV: {err}") from err
    finally:
        if should_close:
            file.close()


def read_parquet(
    source: Union[str, TextIO],
    batch_size: Optional[int] = None
) -> Iterator[Dict[str, Any]]:
    '''
    Read a Parquet file and yield each row as a dict.
    Uses PyArrow for streaming row batches.
    '''
    if isinstance(source, str):
        try:
            pq_file = pq.ParquetFile(source)
        except Exception as err:
            raise RuntimeError(f"Error opening Parquet: {err}") from err
        iterator = pq_file.iter_batches(batch_size=batch_size) if batch_size is not None else pq_file.iter_batches()
        should_close = False
    else:
        raw = source.read() if hasattr(source, 'read') else b''
        buffer_data = raw if isinstance(raw, (bytes, bytearray)) else raw.encode('utf-8')
        file_obj = BytesIO(buffer_data)
        try:
            pq_file = pq.ParquetFile(file_obj)
        except Exception as err:
            raise RuntimeError(f"Error opening Parquet: {err}") from err
        iterator = pq_file.iter_batches(batch_size=batch_size) if batch_size is not None else pq_file.iter_batches()
        should_close = True

    try:
        for batch in iterator:
            for rec in batch.to_pylist():
                yield rec
    except Exception as err:
        raise RuntimeError(f"Error reading Parquet: {err}") from err
    finally:
        if should_close:
            file_obj.close()


def read_dataframe(source: Union[pd.DataFrame, pa.Table]) -> Iterator[Dict[str, Any]]:
    '''
    Read rows from a pandas DataFrame or PyArrow Table, yielding dicts. Raises RuntimeError on unsupported type.
    '''
    try:
        if isinstance(source, pd.DataFrame):
            for rec in source.to_dict(orient='records'):
                yield rec # type: ignore
        elif isinstance(source, pa.Table):
            for batch in source.to_batches():
                for rec in batch.to_pylist():
                    yield rec
        else:
            raise RuntimeError(f"Unsupported type: {type(source)}")
    except Exception as err:
        raise RuntimeError(f"Error reading DataFrame: {err}") from err