"""Source/target configs and pure helpers — path building, format defaults,
JDBC URL construction. No Spark dependency, so all fully unit-testable.

The point of this module: a user shouldn't have to hand-write an abfss://
URI or a JDBC connection string. They pick a source kind and fill a few
plain fields (storage account, container, host, database, ...); these
functions turn that into what Spark actually needs.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Union

from dashingest.readers import SPARK_FORMAT_FOR, build_reader_options, default_reader_options

FILE_EXTENSIONS = {
    "csv": "csv", "tsv": "csv", "json": "json", "jsonl": "json",
    "parquet": "parquet", "avro": "avro", "orc": "orc",
    "xlsx": "excel", "xls": "excel", "txt": "text",
}


@dataclass
class VolumeSource:
    """A Unity Catalog Volume — /Volumes/<catalog>/<schema>/<volume>/<path>."""
    catalog: str
    schema_name: str
    volume: str
    path: str = ""
    file_format: str | None = None  # inferred from path extension if omitted
    reader_options: Any = None      # a CsvReaderOptions/ExcelReaderOptions/... — defaults used if unset
    options: dict = field(default_factory=dict)  # raw Spark options, applied on top of reader_options


@dataclass
class ADLSSource:
    """Azure Data Lake Storage Gen2 — abfss://<container>@<account>.dfs.core.windows.net/<path>."""
    storage_account: str
    container: str
    path: str = ""
    file_format: str | None = None
    reader_options: Any = None
    options: dict = field(default_factory=dict)


@dataclass
class S3Source:
    """Amazon S3 — s3://<bucket>/<path>."""
    bucket: str
    path: str = ""
    file_format: str | None = None
    reader_options: Any = None
    options: dict = field(default_factory=dict)


@dataclass
class DBFSSource:
    """Legacy DBFS mount or path — dbfs:/<path>."""
    path: str
    file_format: str | None = None
    reader_options: Any = None
    options: dict = field(default_factory=dict)


JDBC_PRESETS = {
    "postgresql": {"driver": "org.postgresql.Driver", "url": "jdbc:postgresql://{host}:{port}/{database}", "default_port": 5432},
    "mysql": {"driver": "com.mysql.cj.jdbc.Driver", "url": "jdbc:mysql://{host}:{port}/{database}", "default_port": 3306},
    "sqlserver": {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "url": "jdbc:sqlserver://{host}:{port};databaseName={database}", "default_port": 1433},
    "oracle": {"driver": "oracle.jdbc.OracleDriver", "url": "jdbc:oracle:thin:@{host}:{port}:{database}", "default_port": 1521},
    "snowflake": {"driver": "net.snowflake.client.jdbc.SnowflakeDriver", "url": "jdbc:snowflake://{host}/?db={database}", "default_port": 443},
}


@dataclass
class DatabaseSource:
    """A relational database table or query.

    Set `engine` + `host` + `database` for a known engine (builds the JDBC
    URL/driver for you), or set `url`/`driver` directly for anything else.
    Set exactly one of `table` or `query`.
    """
    host: str = ""
    database: str = ""
    engine: str = "postgresql"  # postgresql | mysql | sqlserver | oracle | snowflake
    port: int | None = None
    table: str = ""
    query: str = ""
    user: str = ""
    password: str = ""
    url: str = ""       # overrides the engine preset if set
    driver: str = ""    # overrides the engine preset if set


@dataclass
class RestApiSource:
    """A JSON REST API. `json_path` is a dot-path to the records array/dict
    if the payload wraps it (e.g. "data.items"); leave empty for a bare array."""
    url: str
    headers: dict = field(default_factory=dict)
    params: dict = field(default_factory=dict)
    json_path: str = ""


@dataclass
class IngestTarget:
    table: str
    write_mode: str = "append"  # append | overwrite | merge
    schema_evolution: bool = True
    merge_keys: list[str] = field(default_factory=list)


PathSource = Union[VolumeSource, ADLSSource, S3Source, DBFSSource]


def infer_format_from_path(path: str) -> str | None:
    ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
    return FILE_EXTENSIONS.get(ext)


def resolve_path(source: PathSource) -> str:
    if isinstance(source, VolumeSource):
        base = f"/Volumes/{source.catalog}/{source.schema_name}/{source.volume}"
    elif isinstance(source, ADLSSource):
        base = f"abfss://{source.container}@{source.storage_account}.dfs.core.windows.net"
    elif isinstance(source, S3Source):
        base = f"s3://{source.bucket}"
    elif isinstance(source, DBFSSource):
        base = "dbfs:"
    else:
        raise TypeError(f"Not a path-based source: {type(source).__name__}")

    path = source.path.lstrip("/")
    return f"{base}/{path}" if path else base


def resolve_format_and_options(source: PathSource) -> tuple[str, dict]:
    """Returns (spark_format, options) — spark_format is what goes into
    spark.read.format(...) (e.g. "com.crealytics.spark.excel" for excel),
    options is the fully-resolved option dict (reader_options translated to
    real Spark keys, with source.options layered on top as raw overrides)."""
    file_format = source.file_format or infer_format_from_path(source.path)
    if not file_format:
        raise ValueError(
            f"Couldn't infer a file format from path {source.path!r} — set file_format explicitly."
        )
    spark_format = SPARK_FORMAT_FOR.get(file_format, file_format)
    reader_opts = source.reader_options if source.reader_options is not None else default_reader_options(file_format)
    options = {**build_reader_options(file_format, reader_opts), **source.options}
    return spark_format, options


def build_jdbc_url(source: DatabaseSource) -> str:
    if source.url:
        return source.url
    if source.engine not in JDBC_PRESETS:
        raise ValueError(f"Unknown engine {source.engine!r}; set url/driver directly for custom engines.")
    preset = JDBC_PRESETS[source.engine]
    port = source.port or preset["default_port"]
    return preset["url"].format(host=source.host, port=port, database=source.database)


def jdbc_driver(source: DatabaseSource) -> str:
    if source.driver:
        return source.driver
    if source.engine not in JDBC_PRESETS:
        raise ValueError(f"Unknown engine {source.engine!r}; set driver directly for custom engines.")
    return JDBC_PRESETS[source.engine]["driver"]
