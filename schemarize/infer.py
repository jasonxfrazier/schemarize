from typing import Any, Dict, List, Set, Union


def get_value_type(value: Any) -> str:
    try:
        if value is None:
            return "NoneType"
        if isinstance(value, dict):
            return "dict"
        if isinstance(value, list):
            return "list"
        return type(value).__name__
    except Exception as err:
        raise RuntimeError(f"Error inferring type for value {value!r}: {err}") from err


def infer_field_types(values: List[Any]) -> Dict[str, Any]:
    try:
        types: Set[str] = set()
        nullable = False
        dict_records = []
        list_items = []

        for value in values:
            t = get_value_type(value)
            types.add(t)
            if value is None:
                nullable = True
            elif t == "dict":
                dict_records.append(value)
            elif t == "list":
                list_items.extend(value)

        result: Dict[str, Any] = {
            "types": sorted(types),
            "nullable": nullable,
        }

        if dict_records:
            result["schema"] = infer_dict_schema(dict_records)
        if list_items:
            result["list_schema"] = infer_list_schema(list_items)

        return result
    except Exception as err:
        raise RuntimeError(f"Error inferring field types: {err}") from err


def infer_dict_schema(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    try:
        all_keys: Set = set()
        for rec in records:
            if not isinstance(rec, dict):
                raise TypeError(f"Expected dict, got {type(rec).__name__}")
            all_keys.update(rec.keys())
        schema: Dict[str, Any] = {}
        for key in all_keys:
            values = [rec.get(key, None) for rec in records]
            schema[key] = infer_field_types(values)
        return schema
    except Exception as err:
        raise RuntimeError(f"Error inferring schema for dicts: {err}") from err


def infer_list_schema(list_of_values: List[Any]) -> Dict[str, Any]:
    try:
        if not isinstance(list_of_values, list):
            raise TypeError("infer_list_schema expects a list")
        if not list_of_values:
            return {}
        value_types = {get_value_type(x) for x in list_of_values}
        if value_types == {"dict"}:
            return infer_dict_schema(list_of_values)
        else:
            return {"element_types": sorted(value_types)}
    except Exception as err:
        raise RuntimeError(f"Error inferring schema for lists: {err}") from err


def infer_schema(
    data: Union[Dict[str, Any], List[Any], str, int, float, bool, None],
) -> Any:
    """
    Entry point. Accepts:
    - list of dicts (standard record case)
    - dict (single record)
    - list of primitives
    """
    try:
        if isinstance(data, dict):
            return infer_dict_schema([data])
        elif isinstance(data, list):
            if not data:
                return {}
            first_type = get_value_type(data[0])
            if first_type == "dict":
                return infer_dict_schema(data)
            else:
                return infer_list_schema(data)
        else:
            raise RuntimeError("infer_schema expects a dict or list as input")
    except Exception as err:
        raise RuntimeError(f"Error in infer_schema: {err}") from err
