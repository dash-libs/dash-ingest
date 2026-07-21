"""Tests for secret-reference parsing/resolution and per-engine test queries
(no Spark required)."""
from dashingest.connectors import RestApiSource, parse_secret_ref
from dashingest.ingestor import _resolve_secret, _rest_auth_headers, _rest_basic_auth
from dashingest.ingestor import test_query as connection_test_query


# ── parse_secret_ref ─────────────────────────────────────────────────────────

def test_parse_secret_ref_matches():
    assert parse_secret_ref("{{secrets/my-scope/my-key}}") == ("my-scope", "my-key")


def test_parse_secret_ref_none_for_plain_value():
    assert parse_secret_ref("hunter2") is None


def test_parse_secret_ref_none_for_non_string():
    assert parse_secret_ref(None) is None


def test_parse_secret_ref_strips_whitespace():
    assert parse_secret_ref("  {{secrets/s/k}}  ") == ("s", "k")


# ── _resolve_secret (outside Databricks — no dbutils) ─────────────────────────

def test_resolve_secret_passthrough_for_plain_value():
    assert _resolve_secret("plaintext-password") == "plaintext-password"


def test_resolve_secret_raises_outside_databricks():
    import pytest
    with pytest.raises(RuntimeError, match="outside Databricks"):
        _resolve_secret("{{secrets/scope/key}}")


# ── REST auth still works with plain (non-secret) values ──────────────────────

def test_rest_auth_headers_plain_bearer_unaffected():
    src = RestApiSource(url="https://x", auth_type="bearer", bearer_token="abc123")
    assert _rest_auth_headers(src)["Authorization"] == "Bearer abc123"


def test_rest_basic_auth_plain_password_unaffected():
    src = RestApiSource(url="https://x", auth_type="basic", basic_user="alice", basic_password="secret")
    assert _rest_basic_auth(src) == ("alice", "secret")


# ── test_query ────────────────────────────────────────────────────────────────

def test_connection_test_query_default_for_most_engines():
    assert connection_test_query("postgresql") == "SELECT 1 AS connection_test"
    assert connection_test_query("mysql") == "SELECT 1 AS connection_test"


def test_connection_test_query_oracle_uses_dual():
    assert connection_test_query("oracle") == "SELECT 1 AS connection_test FROM DUAL"
