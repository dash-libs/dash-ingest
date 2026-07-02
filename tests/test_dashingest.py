"""Package-level smoke tests (no Spark required)."""


def test_import():
    import dashingest
    assert hasattr(dashingest, "__version__")


def test_launch_importable():
    from dashingest import launch
    assert callable(launch)


def test_run_ingestion_importable():
    from dashingest import run_ingestion
    assert callable(run_ingestion)
