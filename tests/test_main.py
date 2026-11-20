"""Entry-point tests that do not start Spark."""

import pytest

from main import main


def test_check_config_reports_scope(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["check-config"]) == 0
    output = capsys.readouterr().out
    assert "cpcb_caaqm" in output
    assert "PM2.5" in output
    assert "PM10" in output
    assert "Measurement files found:" in output


def test_help_exits_successfully(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["--help"])
    assert raised.value.code == 0
    output = capsys.readouterr().out
    assert "preprocess" in output
    assert "detect-spikes" in output
    assert "analyze" in output
    assert "evaluate-performance" in output
    assert "build-city-series" in output
    assert "fetch-data-gov" in output
    assert "prepare-data-gov" in output


def test_missing_command_is_a_usage_error() -> None:
    with pytest.raises(SystemExit) as raised:
        main([])
    assert raised.value.code == 2
