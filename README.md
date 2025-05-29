# schemarize

**schemarize** is a Python toolkit for automatically inferring and exporting data schemas from a wide variety of tabular and semi-structured data sources. It helps you understand the structure of your data with just a single function call—no tedious inspection or manual column typing required.

## Features

- **Automatic Schema Inference**: Detects types and structure for CSV, JSON, Parquet, DataFrames, and more.
- **Flexible Input**: Supports file paths, file-like objects, `pandas.DataFrame`, and `pyarrow.Table`.
- **Easy Export**: Save your inferred schema to JSON, YAML, or CSV in one line.
- **User-Friendly API**: One call (`schemarize(...)`) gets you a powerful `Schema` object with multiple export methods.

---

## Installation

```bash
pip install schemarize
```

---

## Quick Start

```python
from schemarize.core import schemarize

# Infer schema from a CSV file
schema = schemarize("data.csv", output="json")

# Print a JSON representation of the schema
print(schema.to_json(indent=2))

# Save schema to file
schema.to_yaml("schema.yaml")
schema.to_csv("schema.csv")
```

---

## Usage

### Entry Point: `schemarize`

```python
from schemarize.core import schemarize

schema = schemarize(
    source,         # str | file-like | pandas.DataFrame | pyarrow.Table
    output="json",  # Output type: "json", "yaml", or "csv" (optional)
    sample_size=100 # (optional) Number of records to sample for inference
)
```

- **`source`**: Path to data file (`.csv`, `.jsonl`, `.json`, `.parquet`), file-like object, or already-loaded DataFrame/Table.
- **`output`** (optional): Return schema formatted as a Python `dict` for "json", "yaml", or "csv". Default is `"json"`.
- **`sample_size`** (optional): Limit inference to first N records (for large datasets).

Returns a **Schema** object.

---

### The `Schema` Object

When you call `schemarize(...)`, you get a `Schema` object with several handy methods:

#### Methods

| Method                  | Description                                   |
|-------------------------|-----------------------------------------------|
| `.to_dict()`            | Returns the schema as a Python `dict`.        |
| `.to_json(indent=2)`    | Returns the schema as a JSON string.          |
| `.to_yaml()`            | Returns the schema as a YAML string.          |
| `.to_csv()`             | Returns the schema as a CSV string.           |
| `.to_json(file_path)`   | Saves the schema as JSON to `file_path`.      |
| `.to_yaml(file_path)`   | Saves the schema as YAML to `file_path`.      |
| `.to_csv(file_path)`    | Saves the schema as CSV to `file_path`.       |

#### Example

```python
# Load a DataFrame and infer its schema
import pandas as pd
df = pd.read_csv("users.csv")
schema = schemarize(df)

# View schema as dictionary
schema_dict = schema.to_dict()

# Print YAML representation
print(schema.to_yaml())

# Save JSON schema to disk
schema.to_json("users_schema.json")
```

---

### Supported Input Types

- **File path**: CSV, JSON, JSON Lines, Parquet
- **File-like object**: Must be opened in correct mode
- **pandas.DataFrame**
- **pyarrow.Table**

### Output

- A `Schema` object you can inspect, export, or save in your preferred format.

---

## Advanced Options

- **Sampling**: For huge datasets, set `sample_size` to limit rows for schema inference.
- **Custom Output**: Use the `output` argument to control return type, or just use the export methods.

---

## License

MIT

---

## Contributing

Pull requests welcome! Please make sure code passes `pytest` and is formatted with `black`, `isort`, and `ruff`.

---

## Questions?

Open an issue on [GitHub](https://github.com/your-username/schemarize/issues) or submit a pull request!
