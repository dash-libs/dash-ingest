"""DashIngest interactive UI for Databricks notebooks — pick a source kind,
fill a few plain fields, run. No hand-written URIs or JDBC strings."""
from __future__ import annotations


def launch():
    try:
        import ipywidgets as w
        from IPython.display import display
    except ImportError:
        raise RuntimeError("ipywidgets required. Run: %pip install ipywidgets") from None

    import dashui

    kind_toggle = w.ToggleButtons(
        options=["Databricks Volume", "ADLS Gen2", "Amazon S3", "DBFS", "Database", "REST API"],
        description="Source:",
    )

    # ── Databricks Volume ───────────────────────────────────────────────────
    vol_catalog = w.Text(description="Catalog:")
    vol_schema = w.Text(description="Schema:")
    vol_volume = w.Text(description="Volume:")
    vol_path = w.Text(description="Path:", placeholder="folder/file.csv (optional)")
    vol_box = w.VBox([w.HBox([vol_catalog, vol_schema, vol_volume]), vol_path])

    # ── ADLS Gen2 ────────────────────────────────────────────────────────────
    adls_account = w.Text(description="Storage account:")
    adls_container = w.Text(description="Container:")
    adls_path = w.Text(description="Path:", placeholder="folder/file.csv (optional)")
    adls_box = w.VBox([w.HBox([adls_account, adls_container]), adls_path])

    # ── Amazon S3 ────────────────────────────────────────────────────────────
    s3_bucket = w.Text(description="Bucket:")
    s3_path = w.Text(description="Path:", placeholder="folder/file.csv (optional)")
    s3_box = w.VBox([s3_bucket, s3_path])

    # ── DBFS ─────────────────────────────────────────────────────────────────
    dbfs_path = w.Text(description="Path:", placeholder="folder/file.csv")
    dbfs_box = w.VBox([dbfs_path])

    # ── Database ─────────────────────────────────────────────────────────────
    db_engine = w.Dropdown(options=["postgresql", "mysql", "sqlserver", "oracle", "snowflake"], description="Engine:")
    db_host = w.Text(description="Host:")
    db_database = w.Text(description="Database:")
    db_port = w.Text(description="Port:", placeholder="default for engine")
    db_table = w.Text(description="Table:", placeholder="schema.table")
    db_query = w.Text(description="or Query:", placeholder="SELECT ... (instead of table)")
    db_user = w.Text(description="User:")
    db_password = w.Password(description="Password:")
    db_box = w.VBox([
        w.HBox([db_engine, db_host, db_port]),
        db_database, db_table, db_query,
        w.HBox([db_user, db_password]),
    ])

    # ── REST API ─────────────────────────────────────────────────────────────
    api_url = w.Text(description="URL:", placeholder="https://api.example.com/records")
    api_json_path = w.Text(description="JSON path:", placeholder="data.items (optional)")
    api_box = w.VBox([api_url, api_json_path])

    # ── File format (path-based sources only) ───────────────────────────────
    file_format = w.Dropdown(
        options=["(infer from path)", "csv", "json", "parquet", "excel", "avro", "orc", "text"],
        description="Format:",
    )

    # CSV options
    csv_delimiter = w.Text(description="Delimiter:", value=",")
    csv_header = w.Checkbox(value=True, description="Has header row")
    csv_null_value = w.Text(description="Null marker:", placeholder="e.g. NA (optional)")
    csv_box = w.VBox([w.HBox([csv_delimiter, csv_header]), csv_null_value])

    # Excel options — the format that actually needs this much configuration
    xl_sheet = w.Text(description="Sheet:", value="0", placeholder="name or 0-based index")
    xl_header_row = w.IntText(description="Header row:", value=0, min=0)
    xl_header = w.Checkbox(value=True, description="Has header row")
    xl_password = w.Password(description="Password:", placeholder="if protected (optional)")
    xl_sheets_to_union = w.Text(description="Union sheets:", placeholder="Jan, Feb, Mar (optional — reads+stacks multiple sheets)")
    excel_box = w.VBox([
        w.HBox([xl_sheet, xl_header_row, xl_header]),
        xl_password, xl_sheets_to_union,
    ])

    format_options_panel = w.VBox([])

    def on_format_change(change):
        format_options_panel.children = {"csv": [csv_box], "excel": [excel_box]}.get(change["new"], [])

    file_format.observe(on_format_change, names="value")

    source_panel = w.VBox([vol_box])
    format_row = w.VBox([file_format, format_options_panel])

    def on_kind_change(change):
        kind = change["new"]
        source_panel.children = {
            "Databricks Volume": [vol_box],
            "ADLS Gen2": [adls_box],
            "Amazon S3": [s3_box],
            "DBFS": [dbfs_box],
            "Database": [db_box],
            "REST API": [api_box],
        }[kind]
        format_row.children = [] if kind in ("Database", "REST API") else [file_format, format_options_panel]

    kind_toggle.observe(on_kind_change, names="value")
    on_kind_change({"new": kind_toggle.value})

    # ── Target ───────────────────────────────────────────────────────────────
    target_table = w.Text(description="Target table:", placeholder="catalog.schema.table")
    write_mode = w.ToggleButtons(options=["append", "overwrite", "merge"], description="Write mode:")
    merge_keys = w.Text(description="Merge keys:", placeholder="id, updated_at (merge mode only)", disabled=True)
    schema_evo = w.Checkbox(value=True, description="Allow schema evolution")
    write_mode.observe(lambda c: setattr(merge_keys, "disabled", c["new"] != "merge"), names="value")

    run_btn = dashui.action_button("Run Ingestion", style="success", emoji="▶")
    output = dashui.output_panel()

    def _build_reader_options(fmt):
        from dashingest.readers import CsvReaderOptions, ExcelReaderOptions

        if fmt == "csv":
            return CsvReaderOptions(
                delimiter=csv_delimiter.value or ",",
                header=csv_header.value,
                null_value=csv_null_value.value.strip(),
            )
        if fmt == "excel":
            sheets = [s.strip() for s in xl_sheets_to_union.value.split(",") if s.strip()]
            return ExcelReaderOptions(
                sheet_name=xl_sheet.value.strip() or "0",
                header_row=xl_header_row.value,
                header=xl_header.value,
                workbook_password=xl_password.value,
                sheet_names=sheets,
            )
        return None

    def _build_source():
        from dashingest.connectors import ADLSSource, DatabaseSource, DBFSSource, RestApiSource, S3Source, VolumeSource

        kind = kind_toggle.value
        fmt = None if file_format.value == "(infer from path)" else file_format.value
        reader_opts = _build_reader_options(fmt)

        if kind == "Databricks Volume":
            return VolumeSource(vol_catalog.value.strip(), vol_schema.value.strip(), vol_volume.value.strip(),
                                 vol_path.value.strip(), fmt, reader_opts)
        if kind == "ADLS Gen2":
            return ADLSSource(adls_account.value.strip(), adls_container.value.strip(), adls_path.value.strip(),
                               fmt, reader_opts)
        if kind == "Amazon S3":
            return S3Source(s3_bucket.value.strip(), s3_path.value.strip(), fmt, reader_opts)
        if kind == "DBFS":
            return DBFSSource(dbfs_path.value.strip(), fmt, reader_opts)
        if kind == "Database":
            port = int(db_port.value.strip()) if db_port.value.strip() else None
            return DatabaseSource(
                host=db_host.value.strip(), database=db_database.value.strip(), engine=db_engine.value,
                port=port, table=db_table.value.strip(), query=db_query.value.strip(),
                user=db_user.value.strip(), password=db_password.value,
            )
        return RestApiSource(api_url.value.strip(), json_path=api_json_path.value.strip())

    def on_run(b):
        with output:
            output.clear_output()
            try:
                from dashingest.connectors import IngestTarget
                from dashingest.ingestor import run_ingestion

                target = IngestTarget(
                    table=target_table.value.strip(),
                    write_mode=write_mode.value,
                    schema_evolution=schema_evo.value,
                    merge_keys=[k.strip() for k in merge_keys.value.split(",") if k.strip()],
                )
                result = run_ingestion(_build_source(), target)
                result.display()
            except Exception as e:
                print(f"❌ {e}")

    run_btn.on_click(on_run)

    ui = dashui.card([
        dashui.header("DashIngest — Data Ingestion", library="dashingest", emoji="📥"),
        dashui.section("Step 1: Source"),
        kind_toggle, source_panel, format_row,
        dashui.section("Step 2: Target"),
        target_table, write_mode, merge_keys, schema_evo,
        dashui.section("Step 3: Run"),
        run_btn, output,
    ])
    display(ui)
