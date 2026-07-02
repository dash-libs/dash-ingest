"""Unit tests for ingestor.py's pure-Python helpers (no Spark required —
the rest of ingestor.py needs a live SparkSession and isn't unit tested,
per this suite's convention)."""
from dashingest.connectors import RestApiSource
from dashingest.ingestor import _extract_json_path, _rest_auth_headers, _rest_basic_auth


def test_extract_json_path_nested():
    data = {"data": {"items": [1, 2, 3]}}
    assert _extract_json_path(data, "data.items") == [1, 2, 3]


def test_extract_json_path_empty_returns_whole_payload():
    data = [1, 2, 3]
    assert _extract_json_path(data, "") == [1, 2, 3]


def test_rest_auth_headers_none():
    src = RestApiSource(url="https://x")
    assert _rest_auth_headers(src) == {}


def test_rest_auth_headers_bearer():
    src = RestApiSource(url="https://x", auth_type="bearer", bearer_token="abc123")
    assert _rest_auth_headers(src)["Authorization"] == "Bearer abc123"


def test_rest_auth_headers_api_key_custom_header_name():
    src = RestApiSource(url="https://x", auth_type="api_key", api_key="secret", api_key_header="X-Custom")
    assert _rest_auth_headers(src) == {"X-Custom": "secret"}


def test_rest_auth_headers_preserves_user_headers():
    src = RestApiSource(url="https://x", headers={"Accept": "application/json"}, auth_type="bearer", bearer_token="t")
    headers = _rest_auth_headers(src)
    assert headers["Accept"] == "application/json"
    assert headers["Authorization"] == "Bearer t"


def test_rest_basic_auth_none_when_not_basic():
    src = RestApiSource(url="https://x", auth_type="bearer", bearer_token="t")
    assert _rest_basic_auth(src) is None


def test_rest_basic_auth_returns_tuple():
    src = RestApiSource(url="https://x", auth_type="basic", basic_user="alice", basic_password="secret")
    assert _rest_basic_auth(src) == ("alice", "secret")


def test_rest_basic_auth_missing_user_returns_none():
    src = RestApiSource(url="https://x", auth_type="basic")
    assert _rest_basic_auth(src) is None
