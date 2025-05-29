# Schemarize

**Schemarize** is a Python package to infer, inspect, and serialize the schema of data files and objects—whether they’re JSON, CSV, Parquet, or DataFrames. It’s designed for simplicity: just call one method, get a schema, and export as JSON, YAML, or CSV.

[GITHUB](https://github.com/jasonxfrazier/schemarize)
---

## Installation

Install from PyPI:

```python
pip install schemarize
```

---

## Quickstart

```python
import pandas as pd
from schemarize import schemarize

df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
schema = schemarize(df)

print(schema.to_json())
print(schema.to_yaml())
print(schema.to_csv())

schema.save("my_schema.json")   # or .yaml, .csv
```

You can also pass a file path (CSV, JSON, Parquet, etc.):

```python
schema = schemarize("data.csv")
print(schema.to_dict())
```

---

## Main Function

### `schemarize(data, *, output="json", sample_size=None) -> Schema`

- **data:** File path, DataFrame, Arrow Table, or file-like object.
- **output:** `"json"`, `"yaml"`, or `"csv"` (for future use; `.to_*()` preferred).
- **sample_size:** Optionally limit number of records to inspect.

Returns a `Schema` object.

---

## Schema Methods

The returned `Schema` object provides:

- `to_dict()` – Return the schema as a Python dict.
- `to_json(pretty=True)` – Serialize as JSON string.
- `to_yaml()` – Serialize as YAML string.
- `to_csv()` – Serialize as CSV string.
- `save(path, format=None)` – Save to file, format auto-detected by extension.

### Example: Saving a schema

```python
schema.save("my_schema.yaml")
schema.save("my_schema.csv")
```

---

## Supported Inputs

- **Pandas DataFrames**
- **PyArrow Tables**
- **File paths:** CSV, JSON, JSONL, Parquet (compressed files supported)
- **File-like objects**

---

## Supported Outputs

- JSON
- YAML (requires `pyyaml`)
- CSV (flat field view)

---

## License

[MIT](https://github.com/jasonxfrazier/schemarize/blob/main/LICENSE)

---

## Links
 
- [Issues](https://github.com/jasonxfrazier/schemarize/issues)
