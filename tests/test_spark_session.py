"""Spark checks. Marked so the config suite can run without a JVM."""

import pytest

from utils.spark_session import assert_supported_java


def test_java_home_is_a_spark4_jdk() -> None:
    runtime = assert_supported_java()
    assert runtime.major in {17, 21}
    assert runtime.home is not None


@pytest.mark.spark
def test_check_spark_command(capsys: pytest.CaptureFixture[str]) -> None:
    from main import main

    code = main(["check-spark"])
    captured = capsys.readouterr()
    assert code == 0, captured.out + captured.err
    assert "SQL probe: 1" in captured.out
    assert "Python worker probe: [2]" in captured.out
