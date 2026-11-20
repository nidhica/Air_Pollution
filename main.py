"""Entry point for configuration, download, inspection, preprocessing, spike detection, analytics, and the Spark check."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _bootstrap() -> None:
    source_root = Path(__file__).resolve().parent / "src"
    source_text = str(source_root)
    if source_text not in sys.path:
        sys.path.insert(0, source_text)


_bootstrap()

from utils.config_loader import ConfigError, inventory_inputs, load_config
from utils.spark_session import (
    SparkEnvironmentError,
    assert_supported_java,
    build_spark_session,
    stop_spark,
    verify_spark,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Air Pollution Spike Detection and Spatiotemporal Analytics "
            "in Indian Cities."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "check-config",
        help="Load settings and report whether the XKDR files are present.",
    )
    subparsers.add_parser(
        "check-spark",
        help="Start a local Spark session and run a one-row probe.",
    )
    subparsers.add_parser(
        "probe",
        help="Call GET /v1/meta and GET /v1/files and print status, content type, and Ray ID.",
    )
    subparsers.add_parser(
        "acquire",
        help="Download every XKDR month inside the configured date window. Prefer acquire-xkdr.",
    )
    planner = subparsers.add_parser(
        "acquire-xkdr",
        help="Plan XKDR month downloads. Add --download with an explicit month range to fetch files.",
    )
    planner.add_argument("--download", action="store_true", help="Download only the explicit month selection.")
    planner.add_argument("--month", action="append", default=[], help="One YYYY-MM month. Repeat for several months.")
    planner.add_argument("--start", help="First YYYY-MM month in an explicit range.")
    planner.add_argument("--end", help="Last YYYY-MM month in an explicit range.")
    subparsers.add_parser(
        "inspect",
        help="Read the local Parquet files with the shared Spark session and write a report.",
    )
    subparsers.add_parser(
        "preprocess",
        help="Clean the selected measurements with the shared Spark session and write Parquet.",
    )
    subparsers.add_parser(
        "detect-spikes",
        help="Flag statistical spikes on the cleaned measurements with the shared Spark session.",
    )
    subparsers.add_parser(
        "analyze",
        help="Aggregate annotated spikes by time, station, and place with the shared Spark session.",
    )
    subparsers.add_parser(
        "evaluate-performance",
        help="Time each pipeline stage into a sandbox and write a performance report.",
    )
    subparsers.add_parser(
        "build-city-series",
        help="Aggregate annotated measurements into city daily and hourly Parquet files.",
    )
    subparsers.add_parser(
        "fetch-data-gov",
        help="Download the current data.gov.in air-quality snapshot without changing XKDR files.",
    )
    subparsers.add_parser(
        "prepare-data-gov",
        help="Clean the stored data.gov.in snapshot with Spark. Spike detection is not applied.",
    )
    args = parser.parse_args(argv)

    try:
        if args.command == "check-config":
            return check_config()
        if args.command == "check-spark":
            return check_spark()
        if args.command == "probe":
            return probe_command()
        if args.command == "acquire":
            return acquire_command()
        if args.command == "acquire-xkdr":
            return acquire_xkdr_command(args)
        if args.command == "inspect":
            return inspect_command()
        if args.command == "preprocess":
            return preprocess_command()
        if args.command == "detect-spikes":
            return detect_spikes_command()
        if args.command == "analyze":
            return analyze_command()
        if args.command == "evaluate-performance":
            return evaluate_performance_command()
        if args.command == "build-city-series":
            return build_city_series_command()
        if args.command == "fetch-data-gov":
            return fetch_data_gov_command()
        if args.command == "prepare-data-gov":
            return prepare_data_gov_command()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1
    except SparkEnvironmentError as exc:
        print(f"Spark environment error: {exc}", file=sys.stderr)
        return 1
    return 2


def check_config() -> int:
    config = load_config()
    inventory = inventory_inputs(config)
    pollutants = ", ".join(config.dataset.pollutants)
    print(f"Project: {config.project_name}")
    print(f"Configuration: {config.source_file}")
    print(f"Dataset: {config.dataset.name} ({config.dataset.provider})")
    print(f"Source filter: {config.dataset.source_filter}")
    print(f"Pollutants: {pollutants}")
    print(f"Time zone: {config.dataset.timezone}")
    print(f"Date window: {_date_window(config.dataset.date_start, config.dataset.date_end)}")
    print(
        "Spike baseline: "
        f"{config.spikes.baseline_window} previous valid observations, "
        f"minimum {config.spikes.minimum_baseline}, "
        f"threshold mean + {config.spikes.threshold_sigmas:g} sample standard deviations"
    )
    print(
        "Analytics: station means at or above the "
        f"{config.analytics.elevated_percentile:g} percentile among series with at least "
        f"{config.analytics.elevated_minimum_valid_observations} valid observations; "
        f"unusual periods are mean + {config.analytics.unusual_sigmas:g} sample standard deviations"
    )
    print(f"Measurements glob: {config.paths.measurements_glob}")
    print(f"Measurement files found: {len(inventory.measurement_files)}")
    print(f"Stations file: {_presence(config.paths.stations_file, inventory.stations_present)}")
    print(
        "Parameters file: "
        f"{_presence(config.paths.parameters_file, inventory.parameters_present)}"
    )
    if not inventory.measurement_files:
        print("No measurement files found yet. Download them with: python main.py acquire")
    return 0


def check_spark() -> int:
    config = load_config()
    java = assert_supported_java()
    spark = build_spark_session(config, force_new=True)
    try:
        report = verify_spark(spark)
    finally:
        stop_spark(spark)

    print(f"Spark version: {report['spark_version']}")
    print(f"Java: {java.major} ({java.executable})")
    print(f"Master: {report['master']}")
    print(f"Time zone: {report['timezone']}")
    print(f"SQL probe: {report['sql_probe']}")
    print(f"Python worker probe: {report['python_worker_probe']}")
    if report["sql_probe"] != 1 or report["python_worker_probe"] != [2]:
        print("Spark probe did not return the expected values.", file=sys.stderr)
        return 1
    return 0


def probe_command() -> int:
    from ingestion.acquire import AcquisitionError, format_diagnostic, probe_api

    try:
        diagnostics = probe_api(load_config())
    except AcquisitionError as exc:
        print(f"Acquisition error: {exc}", file=sys.stderr)
        return 1
    failed = False
    for item in diagnostics:
        print(format_diagnostic(item))
        if item.status != 200:
            failed = True
    return 1 if failed else 0


def acquire_xkdr_command(args: argparse.Namespace) -> int:
    from ingestion.acquire import AcquisitionError
    from ingestion.xkdr_plan import MonthRequest, format_plan, months_between, parse_month_token, plan_acquisition

    selected: list[MonthRequest] = []
    try:
        selected = [parse_month_token(token) for token in args.month]
        if args.start or args.end:
            if not args.start or not args.end:
                print("Pass both --start and --end as YYYY-MM.", file=sys.stderr)
                return 2
            selected.extend(months_between(parse_month_token(args.start), parse_month_token(args.end)))
        manifest = plan_acquisition(
            load_config(),
            months=tuple(selected),
            download=bool(args.download),
        )
    except AcquisitionError as exc:
        print(f"Acquisition error: {exc}", file=sys.stderr)
        return 2 if args.download and not selected else 1
    print(format_plan(manifest))
    months = manifest.get("months")
    failed = isinstance(months, list) and any(
        isinstance(item, dict) and item.get("download_status") == "failed" for item in months
    )
    return 1 if failed else 0


def acquire_command() -> int:
    from ingestion.acquire import AcquisitionError, acquire_dataset, format_acquisition_report

    try:
        report = acquire_dataset(load_config())
    except AcquisitionError as exc:
        print(f"Acquisition error: {exc}", file=sys.stderr)
        return 1
    print(format_acquisition_report(report))
    return 0 if report.ok else 1


def inspect_command() -> int:
    from ingestion.inspect import InspectionError, format_inspection_report, inspect_dataset

    config = load_config()
    spark = build_spark_session(config, force_new=True)
    try:
        try:
            report = inspect_dataset(config, spark)
        except InspectionError as exc:
            print(f"Inspection error: {exc}", file=sys.stderr)
            return 1
    finally:
        stop_spark(spark)
    print(format_inspection_report(report))
    return 0 if report.selection_ok else 1


def preprocess_command() -> int:
    from preprocessing.clean import PreprocessingError, format_preprocessing_report, preprocess_dataset

    config = load_config()
    spark = build_spark_session(config, force_new=True)
    try:
        try:
            report = preprocess_dataset(config, spark)
        except PreprocessingError as exc:
            print(f"Preprocessing error: {exc}", file=sys.stderr)
            return 1
    finally:
        stop_spark(spark)
    print(format_preprocessing_report(report))
    return 0


def evaluate_performance_command() -> int:
    import shutil
    import tempfile
    import time

    from performance.report import write_report
    from performance.run import PerformanceError, format_performance_report, run_evaluation

    config = load_config()
    started = time.perf_counter()
    spark = build_spark_session(config, force_new=True)
    startup_seconds = time.perf_counter() - started
    sandbox = Path(tempfile.mkdtemp(prefix="aqi-performance-"))
    document = None
    try:
        try:
            document = run_evaluation(
                config,
                spark,
                sandbox=sandbox,
                session_startup_seconds=startup_seconds,
            )
        except PerformanceError as exc:
            print(f"Performance error: {exc}", file=sys.stderr)
            return 1
    finally:
        stop_spark(spark)
        shutil.rmtree(sandbox, ignore_errors=True)
    if document is None:
        return 1
    json_path = config.paths.interim_dir / "performance_report.json"
    markdown_path = config.paths.project_root / "reports" / "performance.md"
    write_report(document, json_path, markdown_path)
    print(format_performance_report(document))
    print(f"JSON: {json_path}")
    print(f"Markdown: {markdown_path}")
    failed = [stage["name"] for stage in document["stages"] if stage["success"] is not True]
    if failed or document["canonical_outputs_unchanged"] is not True:
        return 1
    return 0


def build_city_series_command() -> int:
    from analytics.spatiotemporal.city_series import CitySeriesError, build_city_series, format_city_series_report

    config = load_config()
    spark = build_spark_session(config, force_new=True)
    try:
        try:
            report = build_city_series(config, spark)
        except CitySeriesError as exc:
            print(f"City series error: {exc}", file=sys.stderr)
            return 1
    finally:
        stop_spark(spark)
    print(format_city_series_report(report))
    return 0


def fetch_data_gov_command() -> int:
    from ingestion.data_gov import DataGovError, fetch_snapshot, format_ingest_report

    config = load_config()
    try:
        report = fetch_snapshot(config, pause_seconds=0.2)
    except DataGovError as exc:
        print(f"data.gov.in error: {exc}", file=sys.stderr)
        return 1
    print(format_ingest_report(report))
    return 0


def prepare_data_gov_command() -> int:
    from analytics.data_gov import DataGovPrepareError, format_prepare_report, prepare_data_gov

    config = load_config()
    spark = build_spark_session(config, force_new=True)
    try:
        try:
            report = prepare_data_gov(config, spark)
        except DataGovPrepareError as exc:
            print(f"data.gov.in preparation error: {exc}", file=sys.stderr)
            return 1
    finally:
        stop_spark(spark)
    print(format_prepare_report(report))
    return 0


def analyze_command() -> int:
    from analytics.spatiotemporal.summarize import AnalyticsError, analyze_dataset, format_analytics_report

    config = load_config()
    spark = build_spark_session(config, force_new=True)
    try:
        try:
            report = analyze_dataset(config, spark)
        except AnalyticsError as exc:
            print(f"Analytics error: {exc}", file=sys.stderr)
            return 1
    finally:
        stop_spark(spark)
    print(format_analytics_report(report))
    return 0


def detect_spikes_command() -> int:
    from analytics.spikes.detect import SpikeDetectionError, detect_spikes, format_spike_report

    config = load_config()
    spark = build_spark_session(config, force_new=True)
    try:
        try:
            report = detect_spikes(config, spark)
        except SpikeDetectionError as exc:
            print(f"Spike detection error: {exc}", file=sys.stderr)
            return 1
    finally:
        stop_spark(spark)
    print(format_spike_report(report))
    return 0


def _date_window(start: object, end: object) -> str:
    start_text = "full archive" if start is None else str(start)
    end_text = "full archive" if end is None else str(end)
    if start is None and end is None:
        return "full local archive"
    return f"{start_text} to {end_text}"


def _presence(path: Path, present: bool) -> str:
    state = "present" if present else "not downloaded yet"
    return f"{path} [{state}]"


if __name__ == "__main__":
    raise SystemExit(main())
