from __future__ import annotations
from typing import Optional


class Ingestor:
    """
    Load data from common sources into Databricks Delta tables.

    Usage::
        ing = Ingestor()
        ing.from_csv("abfss://container@account.dfs.core.windows.net/data/")
        ing.to_table("catalog.schema.target")
        ing.run()
    """

    SUPPORTED_FORMATS = ["csv", "json", "parquet", "excel", "jdbc", "api"]

    def __init__(self):
        self._source_format: Optional[str] = None
        self._source_path: Optional[str] = None
        self._source_options: dict = {}
        self._target_table: Optional[str] = None
        self._write_mode: str = "append"
        self._schema_evolution: bool = True

    def from_csv(self, path: str, header: bool = True, infer_schema: bool = True):
        self._source_format = "csv"
        self._source_path = path
        self._source_options = {"header": str(header), "inferSchema": str(infer_schema)}
        return self

    def from_json(self, path: str):
        self._source_format = "json"
        self._source_path = path
        return self

    def from_parquet(self, path: str):
        self._source_format = "parquet"
        self._source_path = path
        return self

    def from_jdbc(self, url: str, table: str, user: str, password: str, driver: str = None):
        self._source_format = "jdbc"
        self._source_options = {"url": url, "dbtable": table,
                                 "user": user, "password": password}
        if driver:
            self._source_options["driver"] = driver
        return self

    def to_table(self, table: str):
        self._target_table = table
        return self

    def mode(self, write_mode: str):
        """append | overwrite | merge"""
        self._write_mode = write_mode
        return self

    def with_schema_evolution(self, enabled: bool = True):
        self._schema_evolution = enabled
        return self

    def run(self):
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()

        if not self._source_format:
            raise ValueError("No source defined. Call from_csv(), from_json(), etc. first.")
        if not self._target_table:
            raise ValueError("No target defined. Call to_table() first.")

        reader = spark.read.format(self._source_format)
        for k, v in self._source_options.items():
            reader = reader.option(k, v)

        df = reader.load(self._source_path) if self._source_path else reader.load()

        writer = (
            df.write.format("delta")
              .mode("overwrite" if self._write_mode == "overwrite" else "append")
        )
        if self._schema_evolution:
            writer = writer.option("mergeSchema", "true")

        writer.saveAsTable(self._target_table)
        count = spark.table(self._target_table).count()
        print(f"✅ Ingested to {self._target_table}: {count:,} total rows")
        return df
