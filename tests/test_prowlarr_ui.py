"""Tests for prowlarr_ui."""

import importlib


def test_package_importable() -> None:
    assert importlib.import_module("prowlarr_ui")
