"""DashIngest — ADF-style data ingestion for Databricks: pick a source kind
(Volume, ADLS, S3, DBFS, Database, REST API), fill a few plain fields, run."""
from dashingest.connectors import (
    ADLSSource,
    DatabaseSource,
    DBFSSource,
    IngestTarget,
    RestApiSource,
    S3Source,
    VolumeSource,
    build_jdbc_url,
    infer_format_from_path,
    parse_secret_ref,
    resolve_path,
)
from dashingest.ingestor import ConnectionTestResult, IngestResult, preview, run_ingestion, test_connection, test_query
from dashingest.readers import (
    AvroReaderOptions,
    CsvReaderOptions,
    ExcelReaderOptions,
    JsonReaderOptions,
    OrcReaderOptions,
    ParquetReaderOptions,
    TextReaderOptions,
)
from dashingest.ui import env_setup, launch

__version__ = "0.1.2"
__all__ = [
    "ADLSSource",
    "AvroReaderOptions",
    "ConnectionTestResult",
    "CsvReaderOptions",
    "DBFSSource",
    "DatabaseSource",
    "ExcelReaderOptions",
    "IngestResult",
    "IngestTarget",
    "JsonReaderOptions",
    "OrcReaderOptions",
    "ParquetReaderOptions",
    "RestApiSource",
    "S3Source",
    "TextReaderOptions",
    "VolumeSource",
    "build_jdbc_url",
    "env_setup",
    "infer_format_from_path",
    "launch",
    "parse_secret_ref",
    "preview",
    "resolve_path",
    "run_ingestion",
    "test_connection",
    "test_query",
]
