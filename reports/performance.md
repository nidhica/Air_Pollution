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

## Limitations

- The timed stages used one local Spark session on this machine. This is not a distributed cluster benchmark.
- No second hardware configuration or cluster size was timed, so no speedup is reported.
- Spark on this Windows setup cannot commit Parquet through winutils.exe. Cleaned, spike, and analytics outputs are written as one PyArrow file.
- The stages ran one after another in the same process. A later stage may benefit from data that the operating system still has cached.
- These measurements describe pipeline execution. They are not prediction-accuracy scores and are not compared with machine-learning metrics from a research paper.
