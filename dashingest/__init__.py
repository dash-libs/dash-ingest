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
    resolve_path,
)
from dashingest.ingestor import IngestResult, run_ingestion
from dashingest.readers import (
    AvroReaderOptions,
    CsvReaderOptions,
    ExcelReaderOptions,
    JsonReaderOptions,
    OrcReaderOptions,
    ParquetReaderOptions,
    TextReaderOptions,
)
from dashingest.ui import launch

__version__ = "0.1.0"
__all__ = [
    "ADLSSource",
    "AvroReaderOptions",
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
    "infer_format_from_path",
    "launch",
    "resolve_path",
    "run_ingestion",
]
