# Performance evaluation

Generated: 2026-11-02T22:06:01+05:30 (Asia/Kolkata)

This report records one local run. It is not a distributed cluster benchmark, and it does not report a speedup.

## Machine

| Item | Value |
| --- | --- |
| Execution | single local machine |
| CPU | Intel64 Family 6 Model 186 Stepping 3, GenuineIntel |
| CPU count | 12 |
| RAM bytes | 16876888064 |
| OS | Windows-11-10.0.26300-SP0 |
| Python | 3.12.7 |
| Java | 17 (C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot\bin\java.exe) |
| PySpark | 4.3.0 |
| Spark | 4.3.0 |
| Spark master | local[*] |
| Distributed cluster | no |
| Speedup measured | no |

## Dataset

- Date range: 2024-01-01 to 2024-01-31
- Months: 1
- Measurement files: 1
- Input observations: 4002852
- Output observations: 667280
- Candidate spikes: 16058
- Eligible observations: 655383

Saved reports already stored these counts before this timing run:

- Saved input observations: 4002852
- Saved output observations: 667280
- Saved candidate spikes: 16058
- Saved eligible observations: 655383
- Timed counts match saved reports: yes

## Stages

| Stage | Success | Wall-clock seconds | Input rows | Output rows | Input bytes | Output bytes | Input rows/s | Output rows/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| acquisition | yes | 2.126 | unavailable | unavailable | 7703441 | unavailable | unavailable | unavailable |
| inspection | yes | 16.694 | 4002852 | 667280 | 7703341 | 4968 | 239,778.1 | 39,971.3 |
| preprocessing | yes | 54.151 | 4002852 | 667280 | 7703341 | 2504242 | 73,920.3 | 12,322.6 |
| spike_detection | yes | 17.063 | 667280 | 667280 | 2495843 | 17367923 | 39,106.3 | 39,106.3 |
| spatiotemporal_analytics | yes | 20.588 | 667280 | 3725 | 16662058 | 2495792 | 32,411.5 | 180.9 |

Spark session startup seconds: 9.096

Canonical raw, interim, processed, and analytics files unchanged: yes

## Unavailable measurements

- acquisition.output_bytes: no bytes were written because the selected files were already present
- acquisition.network_download_seconds: The selected files were already present, so this timing is the skip check. The original download duration was not recorded.
- acquisition.input_rows_per_second: the row count was not recorded
- acquisition.output_rows_per_second: the row count was not recorded

## Stage notes

### acquisition

- Started: 2026-11-02T22:04:09+05:30
- Ended: 2026-11-02T22:04:12+05:30
- input_kind: XKDR file list
- output_kind: local raw files
- network_download_timed: no
- ignored_keys: 0
- Files:
  - v1/stations.parquet: skipped, 27232 bytes
  - v1/parameters.parquet: skipped, 1366 bytes
  - v1/measurements/year=2024/month=01/data.parquet: skipped, 7674743 bytes

### inspection

- Started: 2026-11-02T22:04:12+05:30
- Ended: 2026-11-02T22:04:28+05:30
- input_kind: raw measurement, station, and parameter files
- output_kind: inspection report
- date_min: 2024-01-01 00:00:00
- date_max: 2024-01-31 23:00:00
- station_count: 504

### preprocessing

- Started: 2026-11-02T22:04:28+05:30
- Ended: 2026-11-02T22:05:22+05:30
- input_kind: raw measurement, station, and parameter files
- output_kind: cleaned measurements Parquet
- output_writer: pyarrow

### spike_detection

- Started: 2026-11-02T22:05:22+05:30
- Ended: 2026-11-02T22:05:39+05:30
- input_kind: cleaned measurements
- output_kind: annotated measurements and spike rows
- eligible_observations: 655383
- candidate_spikes: 16058
- output_writer: pyarrow

### spatiotemporal_analytics

- Started: 2026-11-02T22:05:39+05:30
- Ended: 2026-11-02T22:06:00+05:30
- input_kind: annotated measurements
- output_kind: aggregate tables
- eligible_observations: 655383
- candidate_spikes: 16058
- output_writer: pyarrow
- output_tables: 8

## Later local run: already downloaded CPCB months

Generated from the 2026-11-17 21:38–21:42 IST pipeline on the same class of machine (Windows 11, 12 CPUs, about 16 GB RAM, Python 3.12.7, Java 17, Spark 4.3.0, `local[*]`). This is still one local Spark session. It is not a cluster speedup.

No new XKDR months were downloaded. `AQI_API_KEY` was not set, so `acquire` was not run. The run processed files already stored under `data/raw`.

| Stage | Exit | Wall-clock seconds | Rows written |
| --- | ---: | ---: | ---: |
| preprocess | 0 | 100.7 | 1,101,728 |
| detect-spikes | 0 | 55.0 | 1,101,728 annotated |
| analyze | 0 | 55.8 | analytics tables |
| build-city-series | 0 | 59.2 | 20,549 daily city rows; 471,777 hourly city rows |

Input rows in the downloaded measurement files: 4,441,502. Rows matching CPCB CAAQM and PM2.5/PM10 inside 2024-01-01 to 2025-09-30: 1,101,728. Rows outside that window: 0. Raw files were not modified.

Prepared Parquet under `data/processed` after this run: 36,159,164 bytes. The largest file is the annotated spike table.

A cold read of the prepared tables used by the timeline (`load_timeline`, no Spark) took 0.966 seconds and returned 1,101,728 XKDR rows and 16,055 data.gov.in rows. Opening the dashboard does not recompute these tables. Changing a timeline filter uses the cached frames.

The January 2024 section above remains the earlier one-month baseline. Do not compare the two wall-clock figures as a speedup: the later run reads more months and writes more than one million prepared rows.

## Limitations

- The timed stages used one local Spark session on this machine. This is not a distributed cluster benchmark.
- No second hardware configuration or cluster size was timed, so no speedup is reported.
- Spark on this Windows setup cannot commit Parquet through winutils.exe. Cleaned, spike, and analytics outputs are written as one PyArrow file.
- The stages ran one after another in the same process. A later stage may benefit from data that the operating system still has cached.
- These measurements describe pipeline execution. They are not prediction-accuracy scores and are not compared with machine-learning metrics from a research paper.
