import json
from typing import Any, Dict, Optional

import pandas as pd
import yaml


class Schema:
    def __init__(self, schema: Dict[str, Any]):
        self._schema = schema

    def to_dict(self) -> Dict[str, Any]:
        """Return the schema as a Python dict."""
        return self._schema

    def to_json(self, pretty: bool = True) -> str:
        """Serialize the schema as a JSON string."""
        if pretty:
            return json.dumps(self._schema, indent=2)
        return json.dumps(self._schema)

    def to_yaml(self) -> str:
        """Serialize the schema as a YAML string (requires PyYAML)."""
        if yaml is None:
            raise ImportError(
                "PyYAML is not installed. Please run 'pip install pyyaml'."
            )
        return yaml.dump(self._schema, sort_keys=False)

    def to_csv(self) -> str:
        """Serialize the schema as CSV (flat field list)."""
        # This is a naive implementation; for more complex/nested schemas you might need custom logic.
        df = pd.json_normalize(self._schema)
        return df.to_csv(index=False)

    def save(self, path: str, format: Optional[str] = None):
        """Save the schema to a file as JSON, YAML, or CSV, based on file extension or given format."""
        fmtt = format or path.split(".")[-1].lower()
        if fmtt == "json":
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_json(pretty=True))
        elif fmtt in {"yaml", "yml"}:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_yaml())
        elif fmtt == "csv":
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_csv())
        else:
            raise ValueError(f"Unsupported format: {fmtt}")

    def __repr__(self):
        return f"Schema({self._schema})"


def schemarize() -> int:
    return 0
