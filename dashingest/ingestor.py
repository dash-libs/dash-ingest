"""Runs an ingestion: source config -> Spark DataFrame -> Delta table.

Spark/JDBC/REST touching — kept separate from connectors.py so the path/URL
building logic there stays unit-testable without a SparkSession.
"""
from __future__ import annotations
from dataclasses import dataclass

from dashingest.connectors import (
    DatabaseSource,
    IngestTarget,
    RestApiSource,
    build_jdbc_url,
    jdbc_driver,
    parse_secret_ref,
    resolve_format_and_options,
    resolve_path,
)
from dashingest.readers import ExcelReaderOptions, build_reader_options

# Most JDBC engines accept a bare "SELECT 1"; Oracle requires FROM DUAL.
_TEST_QUERY_FOR = {"oracle": "SELECT 1 AS connection_test FROM DUAL"}
_DEFAULT_TEST_QUERY = "SELECT 1 AS connection_test"


def test_query(engine: str) -> str:
    """The lightweight validation query test_connection() runs for a given
    JDBC engine — most engines accept a bare SELECT, Oracle needs FROM DUAL."""
    return _TEST_QUERY_FOR.get(engine, _DEFAULT_TEST_QUERY)


@dataclass
class IngestResult:
    table: str
    rows_ingested: int  # rows processed in this run, not the target table's total size
    write_mode: str

    def display(self) -> None:
        print(f"Ingested into {self.table} ({self.write_mode}): {self.rows_ingested:,} rows this run")


@dataclass
class ConnectionTestResult:
    ok: bool
    message: str

    def display(self) -> None:
        print(f"{'OK' if self.ok else 'Error'} — {self.message}")


def run_ingestion(source, target: IngestTarget) -> IngestResult:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if target.incremental:
        return _run_incremental(source, target, spark)
    df = _load(source, spark)
    rows = df.count()
    _write(df, target, spark)
    return IngestResult(target.table, rows, target.write_mode)


def _run_incremental(source, target: IngestTarget, spark) -> IngestResult:
    """Auto Loader (cloudFiles) ingestion — only picks up files not already
    seen, instead of re-reading the whole source on every run."""
    if isinstance(source, (DatabaseSource, RestApiSource)):
        raise ValueError(
            "incremental=True is only supported for path-based sources "
            "(Volume/ADLS/S3/DBFS) — Database/REST API sources always read fresh."
        )
    if target.write_mode == "merge":
        raise ValueError(
            "incremental=True doesn't support write_mode='merge' — Auto Loader "
            "appends to the target table; run a downstream MERGE from there if "
            "you need dedup."
        )
    if not target.checkpoint_location:
        raise ValueError(
            "incremental=True requires target.checkpoint_location (a Volume/DBFS "
            "path Auto Loader uses for its checkpoint + schema state)."
        )

    spark_format, options = resolve_format_and_options(source)
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", spark_format)
        .option("cloudFiles.schemaLocation", target.checkpoint_location)
    )
    for key, value in options.items():
        reader = reader.option(key, value)
    df = reader.load(resolve_path(source))

    writer = (
        df.writeStream.format("delta")
        .option("checkpointLocation", target.checkpoint_location)
        .trigger(availableNow=True)
    )
    if target.schema_evolution:
        writer = writer.option("mergeSchema", "true")
    query = writer.toTable(target.table)
    query.awaitTermination()

    rows = sum(p.get("numInputRows", 0) for p in query.recentProgress)
    return IngestResult(target.table, rows, "incremental (Auto Loader)")


def preview(source, limit: int = 10):
    """Load up to `limit` rows without writing anywhere — returns a pandas
    DataFrame for display in a notebook cell or the UI's output panel."""
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    return _load(source, spark).limit(limit).toPandas()


def test_connection(source) -> ConnectionTestResult:
    """Check reachability/credentials without loading real data."""
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if isinstance(source, DatabaseSource):
        return _test_database_connection(source, spark)
    if isinstance(source, RestApiSource):
        return _test_rest_api_connection(source)
    return _test_path_connection(source, spark)


def _test_database_connection(source: DatabaseSource, spark) -> ConnectionTestResult:
    try:
        reader = _jdbc_reader(source, spark).option("dbtable", f"({test_query(source.engine)}) t")
        reader.load().take(1)
        return ConnectionTestResult(True, f"Connected to {source.engine} at {source.host or build_jdbc_url(source)}")
    except Exception as e:
        return ConnectionTestResult(False, f"Connection failed: {e}")


def _test_rest_api_connection(source: RestApiSource) -> ConnectionTestResult:
    import requests

    try:
        response = requests.get(
            source.url,
            headers=_rest_auth_headers(source),
            params=source.params,
            auth=_rest_basic_auth(source),
            timeout=source.timeout_seconds,
        )
        response.raise_for_status()
        return ConnectionTestResult(True, f"Reachable — HTTP {response.status_code}")
    except Exception as e:
        return ConnectionTestResult(False, f"Request failed: {e}")


def _test_path_connection(source, spark) -> ConnectionTestResult:
    try:
        path = resolve_path(source)
        if _path_exists(spark, path):
            return ConnectionTestResult(True, f"Path exists: {path}")
        return ConnectionTestResult(False, f"Path not found: {path}")
    except Exception as e:
        return ConnectionTestResult(False, f"Couldn't check path: {e}")


def _path_exists(spark, path: str) -> bool:
    """Works across Volumes/DBFS/ADLS/S3 alike — they're all Hadoop-compatible
    filesystems on a Databricks cluster, so this needs no dbutils reference."""
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def _load(source, spark):
    if isinstance(source, DatabaseSource):
        return _load_database(source, spark)
    if isinstance(source, RestApiSource):
        return _load_rest_api(source, spark)
    return _load_path(source, spark)


def _load_path(source, spark):
    if isinstance(source.reader_options, ExcelReaderOptions) and source.reader_options.sheet_names:
        return _load_excel_sheets(source, spark)

    spark_format, options = resolve_format_and_options(source)
    reader = spark.read.format(spark_format)
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(resolve_path(source))


def _load_excel_sheets(source, spark):
    """Read several named sheets from the same workbook and union them —
    for the common "one tab per month/region, same columns" spreadsheet shape."""
    from functools import reduce

    sheet_names = source.reader_options.sheet_names
    path = resolve_path(source)
    dfs = []
    for sheet in sheet_names:
        per_sheet_opts = ExcelReaderOptions(**{**vars(source.reader_options), "sheet_names": [], "sheet_name": sheet})
        options = build_reader_options("excel", per_sheet_opts)
        reader = spark.read.format("com.crealytics.spark.excel")
        for key, value in options.items():
            reader = reader.option(key, value)
        dfs.append(reader.load(path))
    return reduce(lambda left, right: left.unionByName(right), dfs)


def _resolve_secret(value: str) -> str:
    """Resolve a `{{secrets/scope/key}}` reference via dbutils.secrets.get;
    returns `value` unchanged if it isn't a secret reference."""
    ref = parse_secret_ref(value)
    if ref is None:
        return value
    dbutils = _get_dbutils()
    if dbutils is None:
        raise RuntimeError(f"Cannot resolve secret reference {value!r} outside Databricks")
    scope, key = ref
    return dbutils.secrets.get(scope=scope, key=key)


def _get_dbutils():
    try:
        from IPython import get_ipython
        shell = get_ipython()
        if shell and hasattr(shell, "user_ns") and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:
        pass
    return None


def _jdbc_reader(source: DatabaseSource, spark):
    reader = (
        spark.read.format("jdbc")
        .option("url", build_jdbc_url(source))
        .option("driver", jdbc_driver(source))
        .option("user", source.user)
        .option("password", _resolve_secret(source.password))
    )
    if source.fetch_size:
        reader = reader.option("fetchsize", source.fetch_size)
    if source.ssl:
        reader = reader.option("ssl", "true")
    if source.is_partitioned:
        reader = (
            reader.option("partitionColumn", source.partition_column)
            .option("numPartitions", source.num_partitions)
            .option("lowerBound", source.lower_bound)
            .option("upperBound", source.upper_bound)
        )
    for key, value in source.connection_properties.items():
        reader = reader.option(key, value)
    return reader


def _load_database(source: DatabaseSource, spark):
    if bool(source.table) == bool(source.query):
        raise ValueError("Set exactly one of table or query on a DatabaseSource.")
    reader = _jdbc_reader(source, spark)
    reader = reader.option("query", source.query) if source.query else reader.option("dbtable", source.table)
    return reader.load()


def _rest_auth_headers(source: RestApiSource) -> dict:
    headers = dict(source.headers)
    if source.auth_type == "bearer" and source.bearer_token:
        headers["Authorization"] = f"Bearer {_resolve_secret(source.bearer_token)}"
    elif source.auth_type == "api_key" and source.api_key:
        headers[source.api_key_header] = _resolve_secret(source.api_key)
    return headers


def _rest_basic_auth(source: RestApiSource):
    if source.auth_type == "basic" and source.basic_user:
        return (source.basic_user, _resolve_secret(source.basic_password))
    return None


def _extract_json_path(data, json_path: str):
    for key in filter(None, json_path.split(".")):
        data = data[key]
    return data


def _load_rest_api(source: RestApiSource, spark):
    import requests

    all_records: list = []
    params = dict(source.params)
    page = 1
    cursor = None

    while True:
        if source.pagination == "page_param":
            params[source.page_param] = page
            if source.page_size_param and source.page_size:
                params[source.page_size_param] = source.page_size
        elif source.pagination == "cursor" and cursor is not None:
            params[source.cursor_param] = cursor

        response = requests.get(
            source.url,
            headers=_rest_auth_headers(source),
            params=params,
            auth=_rest_basic_auth(source),
            timeout=source.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        records = _extract_json_path(data, source.json_path)
        if isinstance(records, dict):
            records = [records]
        if not records:
            break
        all_records.extend(records)

        if source.pagination == "page_param":
            page += 1
            if page > source.max_pages or len(records) == 0:
                break
        elif source.pagination == "cursor":
            cursor = _extract_json_path(data, source.cursor_json_path) if source.cursor_json_path else None
            page += 1
            if not cursor or page > source.max_pages:
                break
        else:
            break

    return spark.createDataFrame(all_records)


def _write(df, target: IngestTarget, spark) -> None:
    if target.write_mode == "merge":
        _merge_write(df, target, spark)
        return

    writer = df.write.format("delta").mode(target.write_mode)
    if target.schema_evolution:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(target.table)


def _merge_write(df, target: IngestTarget, spark) -> None:
    if not target.merge_keys:
        raise ValueError("merge write mode requires merge_keys.")

    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(target.table):
        df.write.format("delta").saveAsTable(target.table)
        return

    delta_table = DeltaTable.forName(spark, target.table)
    condition = " AND ".join(f"target.{key} = source.{key}" for key in target.merge_keys)
    merge = delta_table.alias("target").merge(df.alias("source"), condition)
    if target.schema_evolution and hasattr(merge, "withSchemaEvolution"):
        merge = merge.withSchemaEvolution()  # Delta 3.1+; older runtimes just skip it
    merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
