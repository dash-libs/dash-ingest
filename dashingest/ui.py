"""DashIngest interactive UI for Databricks notebooks — pick a source kind,
fill a few plain fields, run. No hand-written URIs or JDBC strings."""
from __future__ import annotations

_LIBRARY = "dashingest"


def env_setup() -> None:
    """Open the environment setup panel — where should dashingest read/write
    its configs? Defaults to the notebook's current working directory if
    never called."""
    try:
        import dashui
        from IPython.display import display
    except ImportError:
        raise RuntimeError("ipywidgets required. Run: %pip install ipywidgets") from None

    display(dashui.card([
        dashui.header("DashIngest — Environment Setup", library=_LIBRARY),
        dashui.env_setup_panel(_LIBRARY).widget,
    ]))


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

    db_ssl = w.Checkbox(value=False, description="Use SSL")
    db_fetch_size = w.IntText(description="Fetch size:", value=0, layout=w.Layout(width="180px"))
    db_num_partitions = w.IntText(description="Partitions:", value=0, layout=w.Layout(width="180px"))
    db_partition_col = w.Text(description="Partition col:", placeholder="id")
    db_lower_bound = w.IntText(description="Lower bound:", layout=w.Layout(width="180px"))
    db_upper_bound = w.IntText(description="Upper bound:", layout=w.Layout(width="180px"))
    db_props_table = dashui.editable_table(["Property", "Value"], placeholders={"Property": "sslmode", "Value": "require"})
    db_advanced = w.Accordion(children=[w.VBox([
        db_ssl,
        w.HTML("<div style='font-size:12px;color:#5A6872;margin:6px 0 2px'>Parallel read (set all four to split a large table across partitions)</div>"),
        w.HBox([db_num_partitions, db_partition_col]),
        w.HBox([db_lower_bound, db_upper_bound]),
        db_fetch_size,
        w.HTML("<div style='font-size:12px;color:#5A6872;margin:6px 0 2px'>Extra JDBC connection properties</div>"),
        db_props_table.widget,
    ])])
    db_advanced.set_title(0, "Advanced")
    db_advanced.selected_index = None

    db_box = w.VBox([
        w.HBox([db_engine, db_host, db_port]),
        db_database, db_table, db_query,
        w.HBox([db_user, db_password]),
        db_advanced,
    ])

    # ── REST API ─────────────────────────────────────────────────────────────
    api_url = w.Text(description="URL:", placeholder="https://api.example.com/records")
    api_json_path = w.Text(description="JSON path:", placeholder="data.items (optional)")

    api_auth_type = w.Dropdown(options=["none", "bearer", "api_key", "basic"], description="Auth:")
    api_bearer_token = w.Password(description="Token:", disabled=True)
    api_key_header = w.Text(description="Header name:", value="X-API-Key", disabled=True)
    api_key_value = w.Password(description="Key:", disabled=True)
    api_basic_user = w.Text(description="User:", disabled=True)
    api_basic_password = w.Password(description="Password:", disabled=True)
    api_auth_box = w.VBox([api_bearer_token])

    def on_auth_type_change(change):
        api_auth_box.children = {
            "bearer": [api_bearer_token],
            "api_key": [api_key_header, api_key_value],
            "basic": [api_basic_user, api_basic_password],
        }.get(change["new"], [])

    api_auth_type.observe(on_auth_type_change, names="value")

    api_pagination = w.Dropdown(options=["none", "page_param", "cursor"], description="Pagination:")
    api_page_param = w.Text(description="Page param:", value="page", disabled=True)
    api_max_pages = w.IntText(description="Max pages:", value=20, disabled=True)
    api_cursor_param = w.Text(description="Cursor param:", value="cursor", disabled=True)
    api_cursor_json_path = w.Text(description="Cursor JSON path:", placeholder="meta.next_cursor", disabled=True)
    api_pagination_box = w.VBox([])

    def on_pagination_change(change):
        for field in (api_page_param, api_max_pages, api_cursor_param, api_cursor_json_path):
            field.disabled = True
        if change["new"] == "page_param":
            api_page_param.disabled = api_max_pages.disabled = False
            api_pagination_box.children = [api_page_param, api_max_pages]
        elif change["new"] == "cursor":
            api_cursor_param.disabled = api_cursor_json_path.disabled = api_max_pages.disabled = False
            api_pagination_box.children = [api_cursor_param, api_cursor_json_path, api_max_pages]
        else:
            api_pagination_box.children = []

    api_pagination.observe(on_pagination_change, names="value")

    api_headers_table = dashui.editable_table(["Header", "Value"], placeholders={"Header": "Accept", "Value": "application/json"})
    api_params_table = dashui.editable_table(["Param", "Value"])

    api_advanced = w.Accordion(children=[w.VBox([
        api_auth_type, api_auth_box,
        api_pagination, api_pagination_box,
        w.HTML("<div style='font-size:12px;color:#5A6872;margin:6px 0 2px'>Headers</div>"),
        api_headers_table.widget,
        w.HTML("<div style='font-size:12px;color:#5A6872;margin:6px 0 2px'>Query params</div>"),
        api_params_table.widget,
    ])])
    api_advanced.set_title(0, "Advanced")
    api_advanced.selected_index = None

    api_box = w.VBox([api_url, api_json_path, api_advanced])

    # ── File format (path-based sources only) ───────────────────────────────
    file_format = w.Dropdown(
        options=["(infer from path)", "csv", "json", "parquet", "excel", "avro", "orc", "text"],
        description="Format:",
    )

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

    test_btn = dashui.action_button("Test Connection", style="info")
    preview_btn = dashui.action_button("Preview", style="info")
    run_btn = dashui.action_button("Run Ingestion", style="success")
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
            props = {r["Property"]: r["Value"] for r in db_props_table.values()}
            return DatabaseSource(
                host=db_host.value.strip(), database=db_database.value.strip(), engine=db_engine.value,
                port=port, table=db_table.value.strip(), query=db_query.value.strip(),
                user=db_user.value.strip(), password=db_password.value,
                ssl=db_ssl.value,
                fetch_size=db_fetch_size.value or None,
                num_partitions=db_num_partitions.value or None,
                partition_column=db_partition_col.value.strip(),
                lower_bound=db_lower_bound.value if db_num_partitions.value else None,
                upper_bound=db_upper_bound.value if db_num_partitions.value else None,
                connection_properties=props,
            )

        headers = {r["Header"]: r["Value"] for r in api_headers_table.values()}
        params = {r["Param"]: r["Value"] for r in api_params_table.values()}
        return RestApiSource(
            api_url.value.strip(), headers=headers, params=params, json_path=api_json_path.value.strip(),
            auth_type=api_auth_type.value,
            bearer_token=api_bearer_token.value,
            api_key_header=api_key_header.value.strip() or "X-API-Key",
            api_key=api_key_value.value,
            basic_user=api_basic_user.value.strip(),
            basic_password=api_basic_password.value,
            pagination=api_pagination.value,
            page_param=api_page_param.value.strip() or "page",
            max_pages=api_max_pages.value or 20,
            cursor_param=api_cursor_param.value.strip() or "cursor",
            cursor_json_path=api_cursor_json_path.value.strip(),
        )

    # ── Config persistence — structural fields only, never passwords/tokens ──
    def _collect_state() -> dict:
        return {
            "kind": kind_toggle.value,
            "format": file_format.value,
            "volume": {"catalog": vol_catalog.value, "schema": vol_schema.value, "volume": vol_volume.value, "path": vol_path.value},
            "adls": {"account": adls_account.value, "container": adls_container.value, "path": adls_path.value},
            "s3": {"bucket": s3_bucket.value, "path": s3_path.value},
            "dbfs": {"path": dbfs_path.value},
            "database": {
                "engine": db_engine.value, "host": db_host.value, "database": db_database.value,
                "port": db_port.value, "table": db_table.value, "query": db_query.value, "user": db_user.value,
                "ssl": db_ssl.value,
            },
            "rest_api": {"url": api_url.value, "json_path": api_json_path.value, "auth_type": api_auth_type.value, "pagination": api_pagination.value},
            "csv": {"delimiter": csv_delimiter.value, "header": csv_header.value, "null_value": csv_null_value.value},
            "excel": {"sheet": xl_sheet.value, "header_row": xl_header_row.value, "header": xl_header.value},
            "target": {
                "table": target_table.value, "write_mode": write_mode.value,
                "merge_keys": merge_keys.value, "schema_evolution": schema_evo.value,
            },
        }

    def _apply_state(state: dict) -> None:
        if not state:
            return
        kind_toggle.value = state.get("kind", kind_toggle.value)
        file_format.value = state.get("format", file_format.value)
        v = state.get("volume", {})
        vol_catalog.value, vol_schema.value, vol_volume.value, vol_path.value = (
            v.get("catalog", ""), v.get("schema", ""), v.get("volume", ""), v.get("path", ""))
        a = state.get("adls", {})
        adls_account.value, adls_container.value, adls_path.value = (
            a.get("account", ""), a.get("container", ""), a.get("path", ""))
        s3 = state.get("s3", {})
        s3_bucket.value, s3_path.value = s3.get("bucket", ""), s3.get("path", "")
        dbfs_path.value = state.get("dbfs", {}).get("path", "")
        d = state.get("database", {})
        db_engine.value = d.get("engine", db_engine.value)
        db_host.value, db_database.value, db_port.value = d.get("host", ""), d.get("database", ""), d.get("port", "")
        db_table.value, db_query.value, db_user.value = d.get("table", ""), d.get("query", ""), d.get("user", "")
        db_ssl.value = d.get("ssl", False)
        r = state.get("rest_api", {})
        api_url.value, api_json_path.value = r.get("url", ""), r.get("json_path", "")
        api_auth_type.value = r.get("auth_type", api_auth_type.value)
        api_pagination.value = r.get("pagination", api_pagination.value)
        c = state.get("csv", {})
        csv_delimiter.value = c.get("delimiter", ",")
        csv_header.value = c.get("header", True)
        csv_null_value.value = c.get("null_value", "")
        e = state.get("excel", {})
        xl_sheet.value = e.get("sheet", "0")
        xl_header_row.value = e.get("header_row", 0)
        xl_header.value = e.get("header", True)
        t = state.get("target", {})
        target_table.value = t.get("table", "")
        write_mode.value = t.get("write_mode", write_mode.value)
        merge_keys.value = t.get("merge_keys", "")
        schema_evo.value = t.get("schema_evolution", True)

    def _save_state() -> None:
        try:
            dashui.save_config(_LIBRARY, _collect_state())
        except Exception:
            pass  # persistence is a convenience, never block the actual operation on it

    _apply_state(dashui.load_config(_LIBRARY))

    def on_test(b):
        with output:
            output.clear_output()
            try:
                from dashingest.ingestor import test_connection
                source = _build_source()
                _save_state()
                test_connection(source).display()
            except Exception as e:
                print(f"Error: {e}")

    def on_preview(b):
        with output:
            output.clear_output()
            try:
                from dashingest.ingestor import preview
                source = _build_source()
                _save_state()
                print(preview(source, limit=10))
            except Exception as e:
                print(f"Error: {e}")

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
                source = _build_source()
                _save_state()
                result = run_ingestion(source, target)
                result.display()
            except Exception as e:
                print(f"Error: {e}")

    test_btn.on_click(on_test)
    preview_btn.on_click(on_preview)
    run_btn.on_click(on_run)

    env_accordion = w.Accordion(children=[dashui.env_setup_panel(_LIBRARY).widget])
    env_accordion.set_title(0, "Environment setup")
    env_accordion.selected_index = None

    ui = dashui.card([
        dashui.header("DashIngest — Data Ingestion", library="dashingest"),
        env_accordion,
        dashui.section("Step 1: Source"),
        kind_toggle, source_panel, format_row,
        w.HBox([test_btn, preview_btn]),
        dashui.section("Step 2: Target"),
        target_table, write_mode, merge_keys, schema_evo,
        dashui.section("Step 3: Run"),
        run_btn, output,
    ])
    display(ui)
