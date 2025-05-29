from typing import Any, Optional

from .infer import infer_schema
from .readers import read_data
from .schema import Schema


def schemarize(source: Any, sample_size: Optional[int] = None, **kwargs) -> Schema:
    """
    Main entry point for the package.
    Reads any supported data source, infers schema, and returns a Schema object.

    Parameters:
        source: file path, DataFrame, Table, or file-like object
        sample_size: optional max records to sample
        kwargs: reserved for future options

    Returns:
        Schema object (with .to_json(), .to_yaml(), .to_csv(), .save())
    """
    # Read the data into a normalized list of dicts
    data = read_data(source, sample_size=sample_size)
    # Infer the schema structure
    schema_dict = infer_schema(data)
    # Return the Schema object
    return Schema(schema_dict)
