"""Unit tests for Ingestor (no Spark required)."""
import pytest


def test_import():
    import dashingest
    assert hasattr(dashingest, "__version__")


def test_launch_importable():
    from dashingest import launch
    assert callable(launch)


def test_main_class_importable():
    from dashingest import Ingestor
    assert Ingestor is not None
