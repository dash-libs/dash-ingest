"""Unit tests for path/URL building and format inference (no Spark required)."""
import pytest

from dashingest.connectors import (
    ADLSSource,
    DatabaseSource,
    DBFSSource,
    S3Source,
    VolumeSource,
    build_jdbc_url,
    default_format_options,
    infer_format_from_path,
    jdbc_driver,
    resolve_format_and_options,
    resolve_path,
)


# ── path resolution ──────────────────────────────────────────────────────

def test_volume_path_with_subpath():
    src = VolumeSource(catalog="main", schema_name="bronze", volume="landing", path="sales/2024.csv")
    assert resolve_path(src) == "/Volumes/main/bronze/landing/sales/2024.csv"


def test_volume_path_without_subpath():
    src = VolumeSource(catalog="main", schema_name="bronze", volume="landing")
    assert resolve_path(src) == "/Volumes/main/bronze/landing"


def test_adls_path():
    src = ADLSSource(storage_account="myacct", container="raw", path="folder/file.parquet")
    assert resolve_path(src) == "abfss://raw@myacct.dfs.core.windows.net/folder/file.parquet"


def test_s3_path():
    src = S3Source(bucket="my-bucket", path="data/file.json")
    assert resolve_path(src) == "s3://my-bucket/data/file.json"


def test_dbfs_path():
    src = DBFSSource(path="/mnt/data/file.csv")
    assert resolve_path(src) == "dbfs:/mnt/data/file.csv"


def test_path_strips_leading_slash_on_subpath():
    src = S3Source(bucket="b", path="/leading/slash.csv")
    assert resolve_path(src) == "s3://b/leading/slash.csv"


# ── format inference ─────────────────────────────────────────────────────

@pytest.mark.parametrize("path,expected", [
    ("data/file.csv", "csv"),
    ("data/file.CSV", "csv"),
    ("data/file.json", "json"),
    ("data/file.jsonl", "json"),
    ("data/file.parquet", "parquet"),
    ("data/file.xlsx", "excel"),
    ("data/file.avro", "avro"),
    ("data/no_extension", None),
    ("", None),
])
def test_infer_format_from_path(path, expected):
    assert infer_format_from_path(path) == expected


def test_resolve_format_and_options_infers_when_unset():
    src = VolumeSource(catalog="c", schema_name="s", volume="v", path="file.csv")
    fmt, options = resolve_format_and_options(src)
    assert fmt == "csv"
    assert options["header"] == "true"


def test_resolve_format_and_options_explicit_overrides_inference():
    src = VolumeSource(catalog="c", schema_name="s", volume="v", path="file.csv", file_format="text")
    fmt, _ = resolve_format_and_options(src)
    assert fmt == "text"


def test_resolve_format_and_options_raises_when_uninferrable():
    src = VolumeSource(catalog="c", schema_name="s", volume="v", path="no_extension")
    with pytest.raises(ValueError, match="Couldn't infer"):
        resolve_format_and_options(src)


def test_user_options_override_defaults():
    src = VolumeSource(catalog="c", schema_name="s", volume="v", path="file.csv", options={"header": "false"})
    _, options = resolve_format_and_options(src)
    assert options["header"] == "false"
    assert options["inferSchema"] == "true"  # untouched default survives


def test_default_format_options_unknown_format_returns_empty():
    assert default_format_options("nonexistent") == {}


# ── JDBC ──────────────────────────────────────────────────────────────────

def test_build_jdbc_url_postgres_default_port():
    src = DatabaseSource(engine="postgresql", host="db.internal", database="analytics")
    assert build_jdbc_url(src) == "jdbc:postgresql://db.internal:5432/analytics"


def test_build_jdbc_url_custom_port():
    src = DatabaseSource(engine="mysql", host="db.internal", database="app", port=3307)
    assert build_jdbc_url(src) == "jdbc:mysql://db.internal:3307/app"


def test_build_jdbc_url_explicit_url_overrides_engine():
    src = DatabaseSource(engine="postgresql", host="ignored", database="ignored", url="jdbc:custom://x")
    assert build_jdbc_url(src) == "jdbc:custom://x"


def test_build_jdbc_url_unknown_engine_without_explicit_url_raises():
    src = DatabaseSource(engine="db2", host="h", database="d")
    with pytest.raises(ValueError, match="Unknown engine"):
        build_jdbc_url(src)


def test_jdbc_driver_from_preset():
    assert jdbc_driver(DatabaseSource(engine="sqlserver")) == "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def test_jdbc_driver_explicit_overrides_preset():
    src = DatabaseSource(engine="postgresql", driver="com.custom.Driver")
    assert jdbc_driver(src) == "com.custom.Driver"
