from __future__ import annotations

import pytest

import prowlarr_ui.app as appmod


class _SignalStub:
    def __init__(self) -> None:
        self._callbacks: list[object] = []

    def connect(self, callback: object) -> None:
        self._callbacks.append(callback)

    def emit(self, *args: object, **kwargs: object) -> None:
        for callback in list(self._callbacks):
            callback(*args, **kwargs)


class _InitWorkerStub:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.init_done = _SignalStub()

    def start(self) -> None:
        return None

    def isRunning(self) -> bool:
        return False

    def requestInterruption(self) -> None:
        return None

    def wait(self, _timeout_ms: int = 0) -> bool:
        return True


@pytest.fixture
def mocked_main():
    return appmod


@pytest.fixture
def window(
    qtbot: object, monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> appmod.MainWindow:
    monkeypatch.setenv("CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(appmod, "InitWorker", _InitWorkerStub)
    widget = appmod.MainWindow()
    qtbot.addWidget(widget)
    widget.populate_indexers(
        [
            {"id": 1, "name": "Indexer One", "enable": True},
            {"id": 2, "name": "Indexer Two", "enable": True},
        ]
    )
    widget.populate_categories(
        widget.prowlarr.get_categories() if widget.prowlarr else []
    )
    widget.everything = object()
    widget.status_label.setText("Ready - 2 indexers loaded")
    yield widget
    widget.close()
