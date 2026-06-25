# Databricks notebook source
# MAGIC %md
# MAGIC # dash-ingest — Data Ingestion
# MAGIC
# MAGIC Ingest CSV, JSON, Parquet and JDBC sources into Delta tables.
# MAGIC
# MAGIC **Install and launch:**

# COMMAND ----------

# MAGIC %pip install dash-ingest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dashingest
dashingest.launch()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Python API (optional — for automation)
# MAGIC
# MAGIC ```python
# MAGIC import dashingest
# MAGIC # See docs/api/ for full API reference
# MAGIC ```
