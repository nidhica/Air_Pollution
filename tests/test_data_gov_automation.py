"""Automation script tests.

These tests run the PowerShell files with a fake python.exe. They do not call
data.gov.in and they do not register a Windows scheduled task.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
RUN_SCRIPT = REPO / "scripts" / "run_data_gov_pipeline.ps1"
INSTALL_SCRIPT = REPO / "scripts" / "install_data_gov_task.ps1"
CHECK_SCRIPT = REPO / "scripts" / "check_data_gov_task.ps1"
UNINSTALL_SCRIPT = REPO / "scripts" / "uninstall_data_gov_task.ps1"
# Fixture only. This is not a data.gov.in credential.
DUMMY_API_KEY = "dummy-not-a-credential"
TASK_NAME = "AEROSCOPE_DataGov_Hourly"

_FAKE_CS = r"""
using System;
using System.IO;
using System.Threading;
public class FakePython {
    public static int Main(string[] args) {
        string dir = Directory.GetCurrentDirectory();
        File.AppendAllText(Path.Combine(dir, "python-calls.txt"), string.Join("\t", args) + Environment.NewLine);
        string planPath = Path.Combine(dir, "python-plan.txt");
        string plan = File.Exists(planPath) ? File.ReadAllText(planPath) : "";
        string joined = string.Join(" ", args);
        if (plan.IndexOf("leak", StringComparison.OrdinalIgnoreCase) >= 0) {
            Console.WriteLine("__LEAK_ENV__");
            Console.WriteLine("__LEAK_ARG__");
        }
        if (plan.IndexOf("stderr_warning", StringComparison.OrdinalIgnoreCase) >= 0) {
            Console.Error.WriteLine("spark-style warning");
        }
        if (joined.IndexOf("fetch-data-gov", StringComparison.Ordinal) >= 0) {
            if (plan.IndexOf("fetch_sleep", StringComparison.Ordinal) >= 0) {
                Thread.Sleep(4000);
            }
            if (plan.IndexOf("fetch_fail", StringComparison.Ordinal) >= 0) {
                return 7;
            }
            return 0;
        }
        if (joined.IndexOf("prepare-data-gov", StringComparison.Ordinal) >= 0) {
            if (plan.IndexOf("prepare_fail", StringComparison.Ordinal) >= 0) {
                return 9;
            }
            return 0;
        }
        return 3;
    }
}
""".replace("__LEAK_ENV__", "DATA_GOV_IN_API_KEY=" + DUMMY_API_KEY).replace(
    "__LEAK_ARG__", "api-key=" + DUMMY_API_KEY
)


def _quote(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _powershell(path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["powershell.exe", "-NoProfile", "-File", str(path), *arguments],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture(scope="module")
def fake_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    work = tmp_path_factory.mktemp("fake-python")
    source = work / "FakePython.cs"
    output = work / "python.exe"
    source.write_text(_FAKE_CS, encoding="ascii")
    compiler = work / "compile.ps1"
    compiler.write_text(
        "param([string]$Source, [string]$Output)\n"
        "Add-Type -TypeDefinition (Get-Content -LiteralPath $Source -Raw) "
        "-OutputAssembly $Output -OutputType ConsoleApplication\n",
        encoding="ascii",
    )
    result = _powershell(compiler, "-Source", str(source), "-Output", str(output))
    if result.returncode != 0 or not output.is_file():
        raise RuntimeError(result.stderr or result.stdout)
    return output


def _project(tmp_path: Path, fake_python: Path, plan: str, *, with_python: bool = True, with_key: bool = True) -> Path:
    root = tmp_path / "project files"
    root.mkdir()
    if with_python:
        scripts = root / ".venv" / "Scripts"
        scripts.mkdir(parents=True)
        shutil.copy2(fake_python, scripts / "python.exe")
    (root / "main.py").write_text("# test stand-in\n", encoding="ascii")
    if with_key:
        (root / ".env").write_text(f"DATA_GOV_IN_API_KEY={SECRET}\n", encoding="ascii")
    (root / "python-plan.txt").write_text(plan, encoding="ascii")
    snapshot = root / "data" / "raw" / "data_gov_in" / "snapshots"
    snapshot.mkdir(parents=True)
    (snapshot / "keep.txt").write_text("keep", encoding="ascii")
    return root


def _run_pipeline(root: Path) -> subprocess.CompletedProcess[str]:
    driver = root / "driver.ps1"
    driver.write_text(
        "\n".join(
            [
                "Remove-Item Env:\\DATA_GOV_IN_API_KEY -ErrorAction SilentlyContinue",
                f". {_quote(RUN_SCRIPT)}",
                f"$code = Invoke-AeroscopeDataGovPipeline -ProjectRoot {_quote(root)}",
                "if ($code -is [array]) { $code = $code[-1] }",
                "exit $code",
                "",
            ]
        ),
        encoding="ascii",
    )
    return _powershell(driver)


def _calls(root: Path) -> list[str]:
    path = root / "python-calls.txt"
    if not path.is_file():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _log_text(root: Path) -> str:
    log_dir = root / "logs" / "data_gov_automation"
    if not log_dir.is_dir():
        return ""
    parts = []
    for path in sorted(log_dir.iterdir()):
        if path.is_file():
            parts.append(path.read_text(encoding="utf-8-sig", errors="replace"))
    return "\n".join(parts)


def _status(root: Path) -> dict[str, object]:
    path = root / "logs" / "data_gov_automation" / "status.json"
    return json.loads(path.read_text(encoding="utf-8-sig"))


def test_scripts_have_valid_powershell_syntax() -> None:
    for path in (RUN_SCRIPT, INSTALL_SCRIPT, CHECK_SCRIPT, UNINSTALL_SCRIPT):
        quoted = str(path).replace("'", "''")
        command = (
            "$parseErrors = $null; $parseTokens = $null; "
            f"[void][System.Management.Automation.Language.Parser]::ParseFile('{quoted}', [ref]$parseTokens, [ref]$parseErrors); "
            "if ($parseErrors -and @($parseErrors).Count -gt 0) { $parseErrors | ForEach-Object { Write-Output $_.ToString() }; exit 1 }; "
            "exit 0"
        )
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", command],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr


def test_fetch_runs_before_prepare_and_keeps_snapshots(tmp_path: Path, fake_python: Path) -> None:
    root = _project(tmp_path, fake_python, "leak")
    old_log = root / "logs" / "data_gov_automation" / "pipeline-20000101T000000.log"
    old_log.parent.mkdir(parents=True)
    old_log.write_text("old", encoding="ascii")
    old = time.time() - (20 * 24 * 60 * 60)
    old_log.touch()
    import os

    os.utime(old_log, (old, old))
    result = _run_pipeline(root)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _calls(root)
    assert len(calls) == 2
    assert "fetch-data-gov" in calls[0]
    assert "prepare-data-gov" in calls[1]
    assert " " in str(root)
    status = _status(root)
    assert status["success"] is True
    assert status["fetch_exit_code"] == 0
    assert status["prepare_exit_code"] == 0
    assert (root / "data" / "raw" / "data_gov_in" / "snapshots" / "keep.txt").read_text(encoding="ascii") == "keep"
    assert not old_log.exists()
    assert SECRET not in _log_text(root)


def test_python_stderr_does_not_abort_a_successful_process(tmp_path: Path, fake_python: Path) -> None:
    root = _project(tmp_path, fake_python, "stderr_warning")
    result = _run_pipeline(root)
    assert result.returncode == 0, result.stdout + result.stderr
    text = _log_text(root)
    assert "spark-style warning" in text
    assert "unexpected error" not in text
    assert "Python process finished: fetch-data-gov exit 0" in text
    assert "Python process finished: prepare-data-gov exit 0" in text
    status = _status(root)
    assert status["success"] is True
    assert status["prepare_exit_code"] == 0


def test_prepare_does_not_run_when_fetch_fails(tmp_path: Path, fake_python: Path) -> None:
    root = _project(tmp_path, fake_python, "fetch_fail")
    result = _run_pipeline(root)
    assert result.returncode == 7
    calls = _calls(root)
    assert len(calls) == 1
    assert "fetch-data-gov" in calls[0]
    status = _status(root)
    assert status["prepare_ran"] is False
    assert status["fetch_exit_code"] == 7
    assert "prepare-data-gov was not started" in str(status["message"])


def test_prepare_failure_is_reported_and_keeps_the_previous_success(tmp_path: Path, fake_python: Path) -> None:
    root = _project(tmp_path, fake_python, "prepare_fail")
    status_path = root / "logs" / "data_gov_automation" / "status.json"
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        '{"last_success_at":"2026-11-17T18:00:00+05:30","success":true}\n',
        encoding="ascii",
    )
    result = _run_pipeline(root)
    assert result.returncode == 9
    calls = _calls(root)
    assert "fetch-data-gov" in calls[0]
    assert "prepare-data-gov" in calls[1]
    status = _status(root)
    assert status["success"] is False
    assert status["prepare_exit_code"] == 9
    assert status["last_success_at"] == "2026-11-17T18:00:00+05:30"


def test_missing_interpreter_is_reported(tmp_path: Path, fake_python: Path) -> None:
    del fake_python
    root = _project(tmp_path, Path("unused"), "", with_python=False)
    result = _run_pipeline(root)
    assert result.returncode == 3
    assert _calls(root) == []
    assert "not found" in str(_status(root)["message"])


def test_second_run_does_not_start_while_the_first_is_active(tmp_path: Path, fake_python: Path) -> None:
    root = _project(tmp_path, fake_python, "fetch_sleep")
    scripts = root / "scripts"
    scripts.mkdir()
    script = scripts / "run_data_gov_pipeline.ps1"
    shutil.copy2(RUN_SCRIPT, script)
    first = subprocess.Popen(
        ["powershell.exe", "-NoProfile", "-File", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.time() + 20
    while time.time() < deadline and not any("fetch-data-gov" in line for line in _calls(root)):
        if first.poll() is not None:
            stdout, stderr = first.communicate()
            raise AssertionError(f"first run ended early\n{stdout}\n{stderr}")
        time.sleep(0.1)
    second = _powershell(script)
    assert second.returncode == 0, second.stdout + second.stderr
    assert sum("fetch-data-gov" in line for line in _calls(root)) == 1
    assert "already active" in _log_text(root)
    stdout, stderr = first.communicate(timeout=30)
    assert first.returncode == 0, stdout + stderr
    calls = _calls(root)
    assert sum("fetch-data-gov" in line for line in calls) == 1
    assert sum("prepare-data-gov" in line for line in calls) == 1
    assert (root / "data" / "raw" / "data_gov_in" / "snapshots" / "keep.txt").is_file()


def test_install_describes_an_hourly_task_without_registering_it(tmp_path: Path) -> None:
    root = tmp_path / "project files"
    scripts = root / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy2(RUN_SCRIPT, scripts / "run_data_gov_pipeline.ps1")
    capture = tmp_path / "registered.json"
    driver = tmp_path / "install-driver.ps1"
    driver.write_text(
        "\n".join(
            [
                "$registrar = {",
                "    param($TaskName, $Action, $Trigger, $Settings, $Principal, $Description, [switch]$Force)",
                "    $rawStart = [string]$Trigger.StartBoundary",
                "    $localStart = [datetimeoffset]::Parse($rawStart).ToLocalTime()",
                "    [ordered]@{",
                "        TaskName = [string]$TaskName",
                "        Arguments = [string]$Action.Arguments",
                "        Execute = [string]$Action.Execute",
                "        WorkingDirectory = [string]$Action.WorkingDirectory",
                "        Interval = [string]$Trigger.Repetition.Interval",
                "        Minute = $localStart.Minute",
                "        Second = $localStart.Second",
                "        Multiple = [string]$Settings.MultipleInstances",
                "        RunLevel = [string]$Principal.RunLevel",
                "        Logon = [string]$Principal.LogonType",
                "        Description = [string]$Description",
                "        Force = [bool]$Force",
                "    } | ConvertTo-Json | Set-Content -LiteralPath " + _quote(capture) + " -Encoding ASCII",
                "}",
                f". {_quote(INSTALL_SCRIPT)}",
                f"$code = Install-AeroscopeDataGovTask -ProjectRoot {_quote(root)} -Registrar $registrar",
                "exit $code",
                "",
            ]
        ),
        encoding="ascii",
    )
    try:
        result = _powershell(driver)
        assert result.returncode == 0, result.stdout + result.stderr
        saved = json.loads(capture.read_text(encoding="utf-8-sig"))
        assert saved["TaskName"] == TASK_NAME
        assert "-ExecutionPolicy Bypass" in saved["Arguments"]
        assert "-NonInteractive" in saved["Arguments"]
        assert "-WindowStyle Hidden" not in saved["Arguments"]
        assert "Set-ExecutionPolicy" not in saved["Arguments"]
        assert SECRET not in saved["Arguments"]
        assert "api-key" not in saved["Arguments"].lower()
        assert saved["WorkingDirectory"] == str(root)
        assert saved["Interval"] == "PT1H"
        assert saved["Minute"] == 0
        assert saved["Second"] == 0
        assert saved["Multiple"] == "IgnoreNew"
        assert saved["RunLevel"] == "Limited"
        assert saved["Logon"] == "Interactive"
        assert "password" not in saved["Description"].lower()
    finally:
        _remove_accidental_task(root)


def test_uninstall_targets_only_the_aeroscope_task(tmp_path: Path) -> None:
    sentinel = tmp_path / "project files" / "data" / "raw" / "data_gov_in" / "snapshots" / "keep.txt"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("keep", encoding="ascii")
    removed = tmp_path / "removed.txt"
    driver = tmp_path / "uninstall-driver.ps1"
    driver.write_text(
        "\n".join(
            [
                "function Get-ScheduledTask {",
                "    param($TaskName, $ErrorAction)",
                "    if ($TaskName -eq 'AEROSCOPE_DataGov_Hourly') {",
                "        return [pscustomobject]@{ TaskName = $TaskName }",
                "    }",
                "    throw \"unexpected task lookup $TaskName\"",
                "}",
                "function Unregister-ScheduledTask {",
                "    param($TaskName, [switch]$Confirm)",
                "    if ($TaskName -ne 'AEROSCOPE_DataGov_Hourly') { throw \"unexpected task $TaskName\" }",
                "    Set-Content -LiteralPath " + _quote(removed) + " -Value $TaskName -Encoding ASCII",
                "}",
                f". {_quote(UNINSTALL_SCRIPT)}",
                "$code = Uninstall-AeroscopeDataGovTask",
                "exit $code",
                "",
            ]
        ),
        encoding="ascii",
    )
    result = _powershell(driver)
    assert result.returncode == 0, result.stdout + result.stderr
    assert removed.read_text(encoding="ascii").strip() == TASK_NAME
    assert sentinel.read_text(encoding="ascii") == "keep"
    uninstall_source = UNINSTALL_SCRIPT.read_text(encoding="utf-8")
    assert "Remove-Item" not in uninstall_source
    assert "Unregister-ScheduledTask -TaskName $AeroscopeDataGovTaskName" in uninstall_source


def test_task_result_codes_are_formatted_without_int32_overflow(tmp_path: Path) -> None:
    driver = tmp_path / "format-results.ps1"
    driver.write_text(
        "\n".join(
            [
                f". {_quote(CHECK_SCRIPT)}",
                "$codes = @(",
                "    0,",
                "    267009,",
                "    267011,",
                "    ([uint32]3221225786),",
                "    -1073741510,",
                "    4000000000,",
                "    999999999999,",
                "    'not-a-code'",
                ")",
                "foreach ($code in $codes) {",
                "    Write-Output (Format-AeroscopeTaskResult -Code $code)",
                "}",
                "",
            ]
        ),
        encoding="ascii",
    )
    result = _powershell(driver)
    assert result.returncode == 0, result.stdout + result.stderr
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    assert lines == [
        "0 (success)",
        "267009 (currently running)",
        "267011 (has not run yet)",
        "3221225786 (interrupted or terminated) 0xC000013A",
        "3221225786 (interrupted or terminated) 0xC000013A",
        "4000000000 0xEE6B2800",
        "999999999999 0xE8D4A50FFF",
        "not-a-code (unrecognized)",
    ]
    assert "success" not in lines[3]


def _remove_accidental_task(root: Path) -> None:
    command = (
        "$task = Get-ScheduledTask -TaskName 'AEROSCOPE_DataGov_Hourly' -ErrorAction SilentlyContinue; "
        "if ($null -eq $task) { exit 0 }; "
        "$action = $task.Actions | Select-Object -First 1; "
        f"if ([string]$action.WorkingDirectory -eq {_quote(root)}) "
        "{ Unregister-ScheduledTask -TaskName 'AEROSCOPE_DataGov_Hourly' -Confirm:$false }; "
        "exit 0"
    )
    subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", command],
        capture_output=True,
        text=True,
        check=False,
    )
