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
    resolve_format_and_options,
    resolve_path,
)


@dataclass
class IngestResult:
    table: str
    row_count: int
    write_mode: str

    def display(self) -> None:
        print(f"✅ Ingested into {self.table} ({self.write_mode}): {self.row_count:,} total rows")


def run_ingestion(source, target: IngestTarget) -> IngestResult:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    df = _load(source, spark)
    _write(df, target, spark)
    count = spark.table(target.table).count()
    return IngestResult(target.table, count, target.write_mode)


def _load(source, spark):
    if isinstance(source, DatabaseSource):
        return _load_database(source, spark)
    if isinstance(source, RestApiSource):
        return _load_rest_api(source, spark)
    return _load_path(source, spark)


def _load_path(source, spark):
    file_format, options = resolve_format_and_options(source)
    reader = spark.read.format(file_format)
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(resolve_path(source))


def _load_database(source: DatabaseSource, spark):
    if bool(source.table) == bool(source.query):
        raise ValueError("Set exactly one of table or query on a DatabaseSource.")
    reader = (
        spark.read.format("jdbc")
        .option("url", build_jdbc_url(source))
        .option("driver", jdbc_driver(source))
        .option("user", source.user)
        .option("password", source.password)
    )
    reader = reader.option("query", source.query) if source.query else reader.option("dbtable", source.table)
    return reader.load()


def _load_rest_api(source: RestApiSource, spark):
    import requests

    response = requests.get(source.url, headers=source.headers, params=source.params)
    response.raise_for_status()
    data = response.json()
    for key in filter(None, source.json_path.split(".")):
        data = data[key]
    if isinstance(data, dict):
        data = [data]
    return spark.createDataFrame(data)


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
