"""Per-file-type reader configuration — pure logic, no Spark dependency.

Each file format gets its own options dataclass with names that mean
something (not raw Spark option strings), plus a `build_reader_options()`
that translates a dataclass instance into the actual Spark/spark-excel
option keys. Excel gets the most attention here: vanilla Spark has no
native Excel reader, so sheet selection, header row, cell ranges, and
password-protected workbooks all need explicit handling.
"""
from __future__ import annotations
from dataclasses import dataclass, field

# The Spark data source format string to use for spark.read.format(...).
# Excel isn't a builtin — it needs the spark-excel library (bundled on
# Databricks Runtime as a supported format, or installable as a Maven lib).
SPARK_FORMAT_FOR = {
    "csv": "csv",
    "json": "json",
    "parquet": "parquet",
    "avro": "avro",
    "orc": "orc",
    "text": "text",
    "excel": "com.crealytics.spark.excel",
}


@dataclass
class CsvReaderOptions:
    header: bool = True
    infer_schema: bool = True
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: str = "\\"
    encoding: str = "UTF-8"
    null_value: str = ""
    date_format: str = ""
    timestamp_format: str = ""
    multiline: bool = False
    comment_char: str = ""
    # PERMISSIVE keeps malformed rows (nulled out) instead of dropping/failing.
    parse_mode: str = "PERMISSIVE"  # PERMISSIVE | DROPMALFORMED | FAILFAST


@dataclass
class JsonReaderOptions:
    # False = one JSON object per line (JSON Lines); True = pretty-printed /
    # multi-line records, which Spark can't split on newlines.
    multiline: bool = False
    infer_schema: bool = True
    date_format: str = ""
    timestamp_format: str = ""
    parse_mode: str = "PERMISSIVE"
    primitives_as_string: bool = False


@dataclass
class ExcelReaderOptions:
    """Comprehensive spark-excel option coverage — the format that actually
    needs it, since a raw file path alone doesn't tell Spark which sheet,
    where the header row is, or whether the workbook is password-protected."""
    sheet_name: str = "0"          # sheet name, or 0-based index as a string
    header: bool = True
    header_row: int = 0            # 0-indexed row the header lives on (title rows above it are skipped)
    data_address: str = ""         # explicit cell range, e.g. "'Sheet1'!B2:F100" — overrides sheet_name/header_row
    infer_schema: bool = True
    treat_empty_as_null: bool = True
    date_format: str = ""
    timestamp_format: str = ""
    max_rows_in_memory: int | None = None  # set for large .xlsx files to stream instead of loading fully
    workbook_password: str = ""
    # Read and union multiple named sheets with matching schemas into one
    # DataFrame, instead of a single sheet_name/data_address.
    sheet_names: list[str] = field(default_factory=list)


@dataclass
class ParquetReaderOptions:
    merge_schema: bool = False


@dataclass
class AvroReaderOptions:
    pass


@dataclass
class OrcReaderOptions:
    merge_schema: bool = False


@dataclass
class TextReaderOptions:
    line_sep: str = ""
    whole_text: bool = False
    encoding: str = "UTF-8"


READER_OPTIONS_FOR_FORMAT = {
    "csv": CsvReaderOptions,
    "json": JsonReaderOptions,
    "excel": ExcelReaderOptions,
    "parquet": ParquetReaderOptions,
    "avro": AvroReaderOptions,
    "orc": OrcReaderOptions,
    "text": TextReaderOptions,
}


def default_reader_options(file_format: str):
    """A ready-to-use options instance with this format's defaults."""
    cls = READER_OPTIONS_FOR_FORMAT.get(file_format)
    return cls() if cls else None


def _bool_str(value: bool) -> str:
    return "true" if value else "false"


def build_reader_options(file_format: str, opts) -> dict:
    """Translate a reader-options dataclass into actual Spark/spark-excel
    option key/value pairs. Unset/empty fields are omitted so Spark's own
    defaults apply rather than overriding them with an empty string."""
    if opts is None:
        return {}

    if isinstance(opts, CsvReaderOptions):
        result = {
            "header": _bool_str(opts.header),
            "inferSchema": _bool_str(opts.infer_schema),
            "sep": opts.delimiter,
            "quote": opts.quote_char,
            "escape": opts.escape_char,
            "encoding": opts.encoding,
            "multiLine": _bool_str(opts.multiline),
            "mode": opts.parse_mode,
        }
        if opts.null_value:
            result["nullValue"] = opts.null_value
        if opts.date_format:
            result["dateFormat"] = opts.date_format
        if opts.timestamp_format:
            result["timestampFormat"] = opts.timestamp_format
        if opts.comment_char:
            result["comment"] = opts.comment_char
        return result

    if isinstance(opts, JsonReaderOptions):
        result = {
            "multiLine": _bool_str(opts.multiline),
            "inferSchema": _bool_str(opts.infer_schema),
            "mode": opts.parse_mode,
            "primitivesAsString": _bool_str(opts.primitives_as_string),
        }
        if opts.date_format:
            result["dateFormat"] = opts.date_format
        if opts.timestamp_format:
            result["timestampFormat"] = opts.timestamp_format
        return result

    if isinstance(opts, ExcelReaderOptions):
        result = {
            "header": _bool_str(opts.header),
            "inferSchema": _bool_str(opts.infer_schema),
            "treatEmptyValuesAsNulls": _bool_str(opts.treat_empty_as_null),
        }
        result["dataAddress"] = opts.data_address or _excel_data_address(opts.sheet_name, opts.header_row)
        if opts.date_format:
            result["dateFormat"] = opts.date_format
        if opts.timestamp_format:
            result["timestampFormat"] = opts.timestamp_format
        if opts.max_rows_in_memory is not None:
            result["maxRowsInMemory"] = str(opts.max_rows_in_memory)
        if opts.workbook_password:
            result["workbookPassword"] = opts.workbook_password
        return result

    if isinstance(opts, ParquetReaderOptions):
        return {"mergeSchema": _bool_str(opts.merge_schema)}

    if isinstance(opts, OrcReaderOptions):
        return {"mergeSchema": _bool_str(opts.merge_schema)}

    if isinstance(opts, AvroReaderOptions):
        return {}

    if isinstance(opts, TextReaderOptions):
        result = {"wholetext": _bool_str(opts.whole_text), "encoding": opts.encoding}
        if opts.line_sep:
            result["lineSep"] = opts.line_sep
        return result

    raise TypeError(f"Unsupported reader options type: {type(opts).__name__}")


def _excel_data_address(sheet_name: str, header_row: int) -> str:
    """Build a spark-excel dataAddress from a sheet name/index + header row.
    header_row=2 means the real header starts on row 3 (1-indexed in Excel),
    skipping two title/banner rows above it."""
    sheet = f"'{sheet_name}'" if not sheet_name.isdigit() else sheet_name
    return f"{sheet}!A{header_row + 1}"
