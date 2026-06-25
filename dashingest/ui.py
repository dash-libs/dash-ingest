"""DashIngest interactive UI for Databricks notebooks."""
from __future__ import annotations


def launch():
    try:
        import ipywidgets as w
        from IPython.display import display
    except ImportError:
        raise RuntimeError("ipywidgets required. Run: %pip install ipywidgets")

    format_dd = w.Dropdown(
        options=["CSV", "JSON", "Parquet", "JDBC"],
        description="Format:", layout=w.Layout(width="200px")
    )
    path_input = w.Text(description="Source path:", placeholder="abfss://... or JDBC URL")
    jdbc_table = w.Text(description="Table (JDBC):", disabled=True)
    jdbc_user = w.Text(description="User (JDBC):", disabled=True)
    jdbc_pwd = w.Password(description="Password (JDBC):", disabled=True)

    def on_format(change):
        is_jdbc = change["new"] == "JDBC"
        jdbc_table.disabled = not is_jdbc
        jdbc_user.disabled = not is_jdbc
        jdbc_pwd.disabled = not is_jdbc
        path_input.placeholder = "JDBC URL" if is_jdbc else "abfss://container@account.dfs.core.windows.net/path"

    format_dd.observe(on_format, names="value")

    target_table = w.Text(description="Target table:", placeholder="catalog.schema.table")
    write_mode = w.ToggleButtons(options=["append", "overwrite"], description="Write mode:")
    schema_evo = w.Checkbox(value=True, description="Allow schema evolution")

    run_btn = w.Button(description="▶ Run Ingestion", button_style="success",
                       layout=w.Layout(height="40px"))
    output = w.Output()

    def on_run(b):
        with output:
            output.clear_output()
            try:
                from dashingest.ingestor import Ingestor
                ing = Ingestor()
                fmt = format_dd.value
                if fmt == "CSV":
                    ing.from_csv(path_input.value.strip())
                elif fmt == "JSON":
                    ing.from_json(path_input.value.strip())
                elif fmt == "Parquet":
                    ing.from_parquet(path_input.value.strip())
                elif fmt == "JDBC":
                    ing.from_jdbc(path_input.value.strip(), jdbc_table.value.strip(),
                                  jdbc_user.value.strip(), jdbc_pwd.value)
                ing.to_table(target_table.value.strip()) \
                   .mode(write_mode.value) \
                   .with_schema_evolution(schema_evo.value) \
                   .run()
            except Exception as e:
                print(f"❌ {e}")

    run_btn.on_click(on_run)

    ui = w.VBox([
        w.HTML("<h2 style='color:#2E7D32'>📥 DashIngest — Data Ingestion</h2>"),
        w.HTML("<b>Step 1: Source</b>"),
        format_dd, path_input, jdbc_table, jdbc_user, jdbc_pwd,
        w.HTML("<hr><b>Step 2: Target</b>"),
        target_table, write_mode, schema_evo,
        w.HTML("<hr>"), run_btn, output,
    ], layout=w.Layout(padding="16px", border="1px solid #ddd", border_radius="8px"))

    display(ui)
