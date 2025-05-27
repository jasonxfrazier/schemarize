import json, gzip, bz2
from typing import Union, Iterator, Dict, Any, TextIO


def read_jsonl(source: Union[str, TextIO]) -> Iterator[Dict[str, Any]]:
    """
    Read a JSONL source (file path or file‑like) and yield one dict at a time.
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
        file = source
        should_close = False

    try:
        for idx, line in enumerate(file, start=1):
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
        if should_close:
            file.close()
