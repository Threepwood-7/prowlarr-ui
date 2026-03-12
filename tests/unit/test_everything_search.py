from __future__ import annotations

from typing import TYPE_CHECKING

from prowlarr_ui.api import everything_search

if TYPE_CHECKING:
    from pathlib import Path


def test_find_everything_exe_prefers_path_lookup(monkeypatch) -> None:
    monkeypatch.setattr(
        "threep_commons.executables.shutil.which",
        lambda _name: r"C:\bin\Everything.exe",
    )

    assert everything_search.find_everything_exe() == r"C:\bin\Everything.exe"


def test_find_everything_exe_uses_program_files_candidates(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("threep_commons.executables.shutil.which", lambda _name: None)
    exe = tmp_path / "Everything.exe"
    exe.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        everything_search,
        "program_files_candidates",
        lambda _relative: [exe],
    )

    assert everything_search.find_everything_exe() == str(exe)
