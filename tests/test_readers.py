"""Unit tests for per-format reader option translation (no Spark required)."""
import pytest

from dashingest.readers import (
    SPARK_FORMAT_FOR,
    AvroReaderOptions,
    CsvReaderOptions,
    ExcelReaderOptions,
    JsonReaderOptions,
    OrcReaderOptions,
    ParquetReaderOptions,
    TextReaderOptions,
    build_reader_options,
    default_reader_options,
)


def test_build_reader_options_none_returns_empty():
    assert build_reader_options("csv", None) == {}


def test_build_reader_options_unsupported_type_raises():
    with pytest.raises(TypeError, match="Unsupported"):
        build_reader_options("csv", object())


# ── CSV ───────────────────────────────────────────────────────────────────

def test_csv_defaults():
    options = build_reader_options("csv", CsvReaderOptions())
    assert options["header"] == "true"
    assert options["inferSchema"] == "true"
    assert options["sep"] == ","
    assert options["mode"] == "PERMISSIVE"
    assert "nullValue" not in options  # omitted when unset


def test_csv_custom_delimiter_and_null_value():
    options = build_reader_options("csv", CsvReaderOptions(delimiter="|", null_value="NA"))
    assert options["sep"] == "|"
    assert options["nullValue"] == "NA"


def test_csv_parse_mode_dropmalformed():
    options = build_reader_options("csv", CsvReaderOptions(parse_mode="DROPMALFORMED"))
    assert options["mode"] == "DROPMALFORMED"


# ── JSON ──────────────────────────────────────────────────────────────────

def test_json_defaults():
    options = build_reader_options("json", JsonReaderOptions())
    assert options["multiLine"] == "false"
    assert options["inferSchema"] == "true"


def test_json_multiline_and_timestamp_format():
    options = build_reader_options("json", JsonReaderOptions(multiline=True, timestamp_format="yyyy-MM-dd"))
    assert options["multiLine"] == "true"
    assert options["timestampFormat"] == "yyyy-MM-dd"


# ── Excel — the format that actually needs comprehensive coverage ─────────

def test_excel_defaults_target_sheet_zero_row_zero():
    options = build_reader_options("excel", ExcelReaderOptions())
    assert options["dataAddress"] == "0!A1"
    assert options["header"] == "true"
    assert options["treatEmptyValuesAsNulls"] == "true"


def test_excel_named_sheet():
    options = build_reader_options("excel", ExcelReaderOptions(sheet_name="Q1 Sales"))
    assert options["dataAddress"] == "'Q1 Sales'!A1"


def test_excel_header_row_skips_title_rows():
    # header_row=2 means two banner/title rows sit above the real header
    options = build_reader_options("excel", ExcelReaderOptions(sheet_name="Data", header_row=2))
    assert options["dataAddress"] == "'Data'!A3"


def test_excel_explicit_data_address_overrides_sheet_and_header_row():
    options = build_reader_options(
        "excel", ExcelReaderOptions(sheet_name="ignored", header_row=5, data_address="'Sheet1'!B2:F100")
    )
    assert options["dataAddress"] == "'Sheet1'!B2:F100"


def test_excel_password_protected_workbook():
    options = build_reader_options("excel", ExcelReaderOptions(workbook_password="secret"))
    assert options["workbookPassword"] == "secret"


def test_excel_omits_password_when_unset():
    options = build_reader_options("excel", ExcelReaderOptions())
    assert "workbookPassword" not in options


def test_excel_max_rows_in_memory_for_streaming_large_files():
    options = build_reader_options("excel", ExcelReaderOptions(max_rows_in_memory=1000))
    assert options["maxRowsInMemory"] == "1000"


def test_excel_spark_format_uses_crealytics_library():
    assert SPARK_FORMAT_FOR["excel"] == "com.crealytics.spark.excel"


# ── Parquet / ORC / Avro / Text ─────────────────────────────────────────────

def test_parquet_merge_schema():
    assert build_reader_options("parquet", ParquetReaderOptions(merge_schema=True))["mergeSchema"] == "true"


def test_orc_merge_schema():
    assert build_reader_options("orc", OrcReaderOptions(merge_schema=True))["mergeSchema"] == "true"


def test_avro_has_no_options():
    assert build_reader_options("avro", AvroReaderOptions()) == {}


def test_text_defaults():
    options = build_reader_options("text", TextReaderOptions())
    assert options["wholetext"] == "false"
    assert options["encoding"] == "UTF-8"
    assert "lineSep" not in options


def test_text_custom_line_separator():
    options = build_reader_options("text", TextReaderOptions(line_sep="\r\n"))
    assert options["lineSep"] == "\r\n"


# ── defaults registry ────────────────────────────────────────────────────

@pytest.mark.parametrize("file_format,expected_type", [
    ("csv", CsvReaderOptions),
    ("json", JsonReaderOptions),
    ("excel", ExcelReaderOptions),
    ("parquet", ParquetReaderOptions),
    ("orc", OrcReaderOptions),
    ("avro", AvroReaderOptions),
    ("text", TextReaderOptions),
])
def test_default_reader_options_returns_right_type(file_format, expected_type):
    assert isinstance(default_reader_options(file_format), expected_type)


def test_default_reader_options_unknown_format_returns_none():
    assert default_reader_options("nonexistent") is None
