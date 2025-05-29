import pytest

from schemarize.schema import (
    get_value_type,
    infer_dict_schema,
    infer_field_types,
    infer_list_schema,
    infer_schema,
)

# --- "Good" (expected usage) tests ---


def test_infer_schema_simple_primitives():
    records = [
        {"a": 1, "b": "foo"},
        {"a": 2, "b": "bar"},
        {"a": 3, "b": "baz"},
    ]
    result = infer_schema(records)
    assert result == {
        "a": {"types": ["int"], "nullable": False},
        "b": {"types": ["str"], "nullable": False},
    }


def test_infer_schema_nullable_and_missing():
    records = [
        {"x": 1},
        {"x": None},
        {},  # missing key
    ]
    result = infer_schema(records)
    assert result == {
        "x": {"types": ["NoneType", "int"], "nullable": True},
    }


def test_infer_schema_multiple_types():
    records = [
        {"x": 1},
        {"x": 2.5},
        {"x": "three"},
        {"x": None},
    ]
    result = infer_schema(records)
    assert result == {
        "x": {"types": ["NoneType", "float", "int", "str"], "nullable": True},
    }


def test_infer_schema_nested_dicts():
    records = [
        {"user": {"id": 1, "name": "A"}},
        {"user": {"id": 2, "name": "B"}},
        {"user": None},
        {},
    ]
    result = infer_schema(records)
    assert result == {
        "user": {
            "types": ["NoneType", "dict"],
            "nullable": True,
            "schema": {
                "id": {"types": ["int"], "nullable": False},
                "name": {"types": ["str"], "nullable": False},
            },
        }
    }


def test_infer_schema_list_of_dicts():
    records = [
        {"events": [{"ts": 123, "val": 1}, {"ts": 124, "val": 2}]},
        {"events": None},
        {},
    ]
    result = infer_schema(records)
    assert result == {
        "events": {
            "types": ["NoneType", "list"],
            "nullable": True,
            "list_schema": {
                "ts": {"types": ["int"], "nullable": False},
                "val": {"types": ["int"], "nullable": False},
            },
        }
    }


def test_infer_schema_list_of_primitives():
    records = [
        {"tags": ["a", "b"]},
        {"tags": None},
        {"tags": []},
    ]
    result = infer_schema(records)
    assert result == {
        "tags": {
            "types": ["NoneType", "list"],
            "nullable": True,
            "list_schema": {"element_types": ["str"]},
        }
    }


def test_infer_schema_top_level_list_of_primitives():
    data = [1, 2, None, 3]
    result = infer_schema(data)
    assert result == {"element_types": ["NoneType", "int"]}


def test_infer_schema_top_level_dict():
    data = {"foo": 1, "bar": None}
    result = infer_schema(data)
    assert result == {
        "foo": {"types": ["int"], "nullable": False},
        "bar": {"types": ["NoneType"], "nullable": True},
    }


# --- Edge and error tests (as above) ---


def test_infer_schema_rejects_non_list_or_dict():
    with pytest.raises(RuntimeError):
        infer_schema(42)
    with pytest.raises(RuntimeError):
        infer_schema("just a string")


def test_infer_dict_schema_rejects_non_dicts():
    with pytest.raises(RuntimeError):
        infer_dict_schema([1, 2, 3])  # type: ignore
    with pytest.raises(RuntimeError):
        infer_dict_schema([{"x": 1}, 2, {"y": 2}])  # type: ignore


def test_infer_list_schema_rejects_non_list():
    with pytest.raises(RuntimeError):
        infer_list_schema({"not": "a list"})  # type: ignore


def test_infer_field_types_with_unusual_objects():
    class Foo:
        pass

    # Should just return type name as 'Foo'
    res = infer_field_types([Foo()])
    assert "Foo" in res["types"]


def test_infer_field_types_with_unhashable_values():
    # Sets are unhashable in JSON but should still return 'set' as a type
    res = infer_field_types([{1, 2}, None])
    assert "set" in res["types"]
    assert res["nullable"] is True


def test_infer_schema_empty_input():
    assert infer_schema([]) == {}
    assert infer_schema({}) == {}


def test_infer_dict_schema_empty():
    assert infer_dict_schema([]) == {}


def test_infer_list_schema_empty():
    assert infer_list_schema([]) == {}


def test_infer_schema_list_of_mixed_types():
    data = [1, {"foo": "bar"}, None, [1, 2], 3.14]
    res = infer_schema(data)
    assert "element_types" in res
    assert set(res["element_types"]) == {"NoneType", "dict", "float", "int", "list"}


def test_infer_schema_nested_weirdness():
    class Foo:
        pass

    data = [
        {"a": [Foo(), {"b": 2}], "c": None},
        {"a": [], "c": 3.14},
        {"a": None, "c": "hi"},
    ]
    result = infer_schema(data)
    assert "a" in result
    assert "list" in result["a"]["types"]
    assert result["a"]["nullable"] is True


def test_infer_field_types_missing_keys():
    records = [{"a": 1}, {}, {"a": None}]
    res = infer_field_types([rec.get("a") for rec in records])
    assert set(res["types"]) == {"NoneType", "int"}
    assert res["nullable"] is True


def test_get_value_type_everything():
    assert get_value_type(1) == "int"
    assert get_value_type(None) == "NoneType"
    assert get_value_type([1, 2]) == "list"
    assert get_value_type({"x": 2}) == "dict"
    assert get_value_type({1, 2}) == "set"
    assert get_value_type("foo") == "str"

    class Bar:
        pass

    assert get_value_type(Bar()) == "Bar"


def test_infer_schema_list_of_lists():
    data = [[1, 2], ["a", "b"], [None]]
    res = infer_schema(data)
    assert "element_types" in res
    assert res["element_types"] == ["list"]


def test_infer_schema_extreme_depth():
    data = [{"a": [{"b": [{"c": 42}]}]}]
    try:
        res = infer_schema(data)
        assert isinstance(res, dict)
        assert "a" in res
    except RuntimeError:
        pytest.fail("infer_schema failed on deeply nested structure")


def test_infer_field_types_with_mutable_in_list():
    values = [[{"x": 1}], [{"x": 2}], [1, 2], None]
    res = infer_field_types(values)
    assert "list" in res["types"]
    assert res["nullable"] is True
    assert "list_schema" in res
