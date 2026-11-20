# Air Pollution Spike Detection and Spatiotemporal Analytics in Indian Cities

Academic Big Data Analytics project for CPCB CAAQM PM2.5 and PM10. The pipeline downloads the XKDR archive, inspects it, cleans it, flags statistical spikes, aggregates the results, and presents them in a Streamlit dashboard. The configured window in `config/settings.yaml` is 1 January 2024 through 30 September 2025. Local CPCB CAAQM PM2.5 and PM10 files cover all of January 2024, all of August 2025, and 1 September 2025. Months between those files are not downloaded. March 2026 is on disk, but that file contains only US Embassy PM2.5 and is outside this window. Further XKDR months require `AQI_API_KEY`. This repository does not contain air-quality measurements or computed results.

## Question and scope

The later analysis will study PM2.5 and PM10 spikes and their variation across Indian cities and time. The source is the [India Air Quality Database](https://airquality.xkdr.org/) published by XKDR Forum. That archive harmonises two networks. This project uses only the Central Pollution Control Board Continuous Ambient Air Quality Monitoring network, identified by the source value `cpcb_caaqm`. US Embassy monitors (`us_embassy`) stay out of the analysis.

XKDR publishes readings as received: no gap filling and no outlier removal. Cleaning choices belong in later project code and should stay explicit.

## Dataset layout

Bulk files are monthly Parquet, one file per month:

```text
data/raw/v1/measurements/year=YYYY/month=MM/data.parquet
data/raw/v1/stations.parquet
data/raw/v1/parameters.parquet
```

`year` and `month` are hive-style partitions. The XKDR documentation names these measurement fields: `station_id`, `parameter_name`, `collected_at`, `value`, `city_name`, and `source`. `collected_at` is a naive timestamp in Indian Standard Time. Confirm the names against the first file you download before writing transformations. Spark will read each Parquet file's own schema; `config/settings.yaml` does not invent one.

Obtain an API key from [airquality.xkdr.org](https://airquality.xkdr.org/) and follow that site's bulk-download instructions. Place the resulting `v1` directory inside `data/raw`. If a later milestone calls the API, put the key in an untracked `.env` file as `AQI_API_KEY`. This milestone does not call the API and does not read measurements.

Cite the database and both source networks if you use the data:

```text
XKDR Forum. India Air Quality Database. 2026. https://airquality.xkdr.org
Central Pollution Control Board, Continuous Ambient Air Quality Monitoring network.
```

The compilation is released under Creative Commons Attribution 4.0.

## Architecture

Configuration is the source of truth for paths, the CAAQM filter, the pollutant list, and Spark settings. `src/utils/config_loader.py` validates it and resolves paths from the repository root, so modules do not depend on the working directory.

`src/utils/spark_session.py` is the only place that builds a `SparkSession`. Later stages receive that session and an `AppConfig`. They do not start their own sessions. Tests can therefore exercise one package with a fixture, or without Spark at all.

A future run moves data through one direction:

1. **Ingestion** reads the monthly Parquet files and keeps CPCB CAAQM rows for the configured pollutants.
2. **Preprocessing** applies the date window, treats timestamps as IST, and attaches station attributes.
3. **Analytics** detects spikes and builds spatiotemporal summaries. Those packages are separate so each rule can be tested on its own.
4. **Storage** writes Parquet to `data/interim` and `data/processed` by path. The Spark catalog is in-memory, so there is no Hive metastore and no Derby files. That avoids a known failure mode when the project path contains spaces.
5. **Dashboard** reads processed Parquet and presents it with Streamlit and Plotly.

`main.py` currently exposes two checks only: `check-config` and `check-spark`.

```text
AirPollution-BDA/
├── main.py
├── requirements.txt
├── pytest.ini
├── config/
│   └── settings.yaml
├── data/
│   ├── raw/                  XKDR archive (gitignored)
│   ├── interim/              intermediate Parquet (gitignored)
│   ├── processed/            analytical outputs (gitignored)
│   └── spark-warehouse/
├── logs/
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── analytics/
│   │   ├── spikes/
│   │   └── spatiotemporal/
│   ├── storage/
│   ├── dashboard/
│   └── utils/
│       ├── config_loader.py
│       └── spark_session.py
└── tests/
```

## Requirements

- Python 3.10 or newer. Python 3.12 is already installed on the setup machine.
- JDK 17 or 21. PySpark 4 does not run on Java 8.
- The Python packages in `requirements.txt`.

On this machine, `JAVA_HOME` points at JDK 17 and the `java` command on `PATH` is Java 8. Spark follows `JAVA_HOME`. The session factory refuses to start unless that JDK is 17 or 21, so the older executable on `PATH` is not used by accident.

PySpark 4.0.5 or newer is required. On Windows, Python 3.12 crashes in older PySpark releases when a Python worker returns a result (SPARK-53759). The SQL engine can look healthy while `parallelize` and `createDataFrame` fail. `check-spark` runs both a SQL probe and a one-row Python worker probe so that failure is visible. The probes do not read pollution data.

## Installation

From PowerShell, in the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If activation is blocked, allow scripts for the current user and try again:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Confirm the JDK that Spark will actually launch:

```powershell
echo $env:JAVA_HOME
& "$env:JAVA_HOME\bin\java.exe" -version
```

The reported version should be 17 or 21. If `JAVA_HOME` is empty, set it to the JDK 17 installation and open a new terminal before starting Spark.

## Verify the skeleton

Load the configuration and print whether the archive is on disk. A missing archive is expected until you download it, and the command still exits successfully:

```powershell
python main.py check-config
```

Start a local session and run the probes:

```powershell
python main.py check-spark
```

A working installation prints a Spark 4 version, Java 17 or 21, time zone `Asia/Kolkata`, `SQL probe: 1`, and `Python worker probe: [2]`. The Spark UI, when left enabled in `config/settings.yaml`, is at [http://127.0.0.1:4040](http://127.0.0.1:4040) while the session is alive. `check-spark` stops the session before it returns.

On Windows, Spark may log that `winutils.exe` is missing. That warning is from Hadoop's local file helpers. The probe lines above are the success signal. The session factory sets `PYSPARK_PYTHON` to the current interpreter, using its space-free Windows path when the project lives under a folder such as `NIDHI C A`. PySpark's default is `python3`, and on Windows that command is often the Microsoft Store alias, which exits before a Python worker can connect. SQL can succeed in that state because it runs in the JVM.

Run the tests in two groups. The default command skips the JVM. The Spark marker starts one local session:

```powershell
python -m pytest -m "not spark"
python -m pytest -m spark
```

`pytest -m "not spark"` covers configuration, package imports, and `check-config`. It does not need the XKDR files. `pytest -m spark` needs JDK 17 or 21 and the installed PySpark package. The first launch can take a minute while the JVM starts.

## Configuration

Edit `config/settings.yaml` to change pollutants, the optional IST date window, memory, and shuffle partitions. Paths are relative to the repository root. An alternate file can be selected with the `AIR_POLLUTION_CONFIG` environment variable.

`dataset.source_filter` must stay `cpcb_caaqm`. `spark.catalog` must stay `in-memory`. Processed outputs go to explicit Parquet paths under `data/processed`.

Shuffle partitions default to 8 for a laptop running `local[*]`. Increase that value if the same code is pointed at a cluster; `spark.master` is the setting to change.

## Independent modules

| Module | Can be tested without |
| --- | --- |
| `utils.config_loader` | a JVM and the dataset |
| `utils.spark_session` | the dataset; needs JDK 17 or 21 |
| `ingestion.acquire` | the network; tests supply a fake transport |
| `ingestion.xkdr_query` | the network; tests supply a fake transport and do not call `/v1/measurements` |
| `dashboard.query_cache` | the network and the dashboard; tests use a temporary directory |
| `ingestion.data_gov` | the network; tests supply a fake transport and do not call data.gov.in |
| `ingestion.inspect` | the XKDR archive; tests pass the shared session a temporary fixture |
| `preprocessing.clean` | the XKDR archive; tests pass the shared session a temporary fixture |
| `analytics.spikes.detect` | the XKDR archive; tests pass the shared session a temporary fixture |
| `analytics.spatiotemporal.summarize` | the XKDR archive; tests pass the shared session a temporary fixture |
| `dashboard.loading`, `dashboard.filters` | Spark; tests use a temporary analytics fixture |
| `performance.report` | Spark and the dataset; tests build stage records directly |
| `storage` | a JVM and the dataset, until that package gains functions |

Package import tests live in `tests/test_imports.py`. Add a focused test module beside each package as its functions appear. Pass `AppConfig` and `SparkSession` in from the test instead of constructing a session inside the package.

## Download and inspect

XKDR publishes the archive as bulk Parquet. `GET /v1/files` lists objects, and `GET /v1/files/<key>` downloads one. The documented keys are `v1/stations.parquet`, `v1/parameters.parquet`, and `v1/measurements/year=YYYY/month=MM/data.parquet`. A month file contains every source and pollutant. `acquire` stores those bytes under `data/raw/` and does not filter or rewrite them. `inspect` opens them with the shared Spark session, keeps `cpcb_caaqm` rows for PM2.5 and PM10, and writes `data/interim/inspection_report.json`.

Request a key at [airquality.xkdr.org/signup](https://airquality.xkdr.org/signup). Put it in the environment or in an untracked `.env` file:

```powershell
copy .env.example .env
# Edit .env so AQI_API_KEY is your own key, then:
.\.venv\Scripts\python.exe main.py acquire
.\.venv\Scripts\python.exe main.py inspect
```

With `date_start` and `date_end` left null, `acquire` downloads every listed month. Set those dates in `config/settings.yaml` to limit the months. A finished file is skipped. An interrupted file remains `data.parquet.partial` and the next `acquire` continues it with an HTTP Range request.

`acquire-xkdr` is the bounded command. With no extra arguments it only writes `data/interim/xkdr_acquisition_manifest.json` and does not download. Add `--month 2024-02` or `--start 2024-02 --end 2024-02` together with `--download` to fetch an explicit selection. A non-empty raw file is not replaced. The command does not call data.gov.in and does not change the hourly scheduled task.

```powershell
.\.venv\Scripts\python.exe main.py acquire-xkdr
.\.venv\Scripts\python.exe main.py acquire-xkdr --download --month 2024-02
```

`pytest -m "not spark"` includes the acquisition tests and the preprocessing, spike, and analytics rules that do not need a JVM. `pytest -m spark` runs inspection, preprocessing, spike detection, and spatiotemporal aggregation against temporary synthetic fixtures, separate from `data/raw`.

## On-demand measurements query

`ingestion.xkdr_query` is a server-side client for `GET /v1/measurements`. The Timeline uses it for CPCB daily PM2.5 and PM10 aggregates on the most recent 7 days in Asia/Kolkata, including today. Older dates stay on prepared Parquet. The same split applies to every preset and to a custom range. It does not replace `acquire`. The API key is read from `AQI_API_KEY` or `.env` and is sent only as an `Authorization: Bearer` header. A failed or truncated recent request stays unavailable and does not replace those days with prepared history. data.gov.in remains a separate section.

A query accepts an inclusive `start` and `end`, `source` (`cpcb_caaqm` or `us_embassy`), one or more pollutants, `agg` (`raw`, `hourly`, `daily`, or `monthly`), and optional `state`, `city`, and repeatable `station` filters. The only accepted body is `format=parquet`. HTTP 200 is accepted only when `X-Truncated` is false, `X-Row-Count` matches the table, and the documented columns are present. Timeouts, connection failures, text bodies, and truncated responses raise `QueryError` and return no table.

`dashboard.query_cache` stores a completed body under `data/cache/xkdr_query/`. The filename is a SHA-256 of the normalized dates, source, pollutants, aggregation, and location filters. The key is not part of that hash. A write uses a temporary file and replaces the cache file only after the write finishes. Entries expire after seven days unless a caller chooses another TTL. The cache refuses a directory inside `data/raw` or `data/processed`.

A live daily PM2.5 request for 15 February 2024 returned HTTP 200, content type `application/vnd.apache.parquet`, `X-Truncated: false`, and `X-Row-Count` equal to the Parquet row count. The aggregate columns were `mean`, `min`, `max`, and `n`, and the period column was `period_start`. The query client accepts `period_start` as that period column and still requires `mean`, `min`, `max`, and `n`. Raw rows are still expected to contain `station_id`, `parameter_name`, `unit`, `collected_at`, and `value` only. City and coordinates are not joined in this phase.

The live check is `integration/test_xkdr_query_live.py`. It is outside `tests/`, so a normal pytest run does not call the network. It makes one daily PM2.5 request for 15 February 2024 and does not write the response.

```powershell
.\.venv\Scripts\python.exe -m pytest integration/test_xkdr_query_live.py -s
```

## Preprocess

`preprocess` reads the files `acquire` stored, keeps the configured source and pollutants, and writes Parquet under `data/processed/measurements/`. The original files in `data/raw/` are hashed before and after the job and are not rewritten. Invalid measurement values stay in the output with a `measurement_status` column. A missing city or state is filled only when `stations.parquet` has a non-blank value for that `station_id`.

```powershell
.\.venv\Scripts\python.exe main.py preprocess
```

The row counts and enrichment results are written to `data/interim/preprocessing_report.json`. Cleaned rows are saved as one Parquet file, `data/processed/measurements/data.parquet`, with `year` and `month` kept as columns. Spark writes the rows with its Parquet writer into a temporary directory, then replaces `data.parquet` only after that write finishes. An existing file is left in place when the write fails. Spark reads the finished file by its path.

## Spike detection

`detect-spikes` reads the cleaned file. For each station and pollutant it orders rows by `collected_at` and compares the current valid value with the previous 24 valid observations, excluding the current row. A row is flagged when at least 12 of those observations exist and the value is strictly above the sample mean plus 3 sample standard deviations. Rows that share a timestamp are preserved and are not ordered against each other.

```powershell
.\.venv\Scripts\python.exe main.py detect-spikes
```

The annotated table is `data/processed/spikes/annotated/data.parquet`. The spike rows are `data/processed/spikes/spikes_only/data.parquet`. Counts by pollutant, station, and date are written to `data/interim/spike_report.json`.

## Spatiotemporal analytics

`analyze` reads `data/processed/spikes/annotated/data.parquet`. Averages use valid measurements only. A spike rate is `spike_count / eligible_observations`, with the raw `spike_count / observations` kept beside it. Eligible rows are the rows spike detection marked with a non-null `spike_threshold`. Station rankings are within each pollutant and include the eligible count. A station is consistently elevated when its mean valid concentration, not its spike count, is at or above the 90th percentile of station means for that pollutant among series with at least 168 valid observations. A date or clock hour is unusually high when its raw spike count, or separately its eligible spike rate, is strictly above the mean plus 2 sample standard deviations for that pollutant.

City and state names are used only when a station has one non-null value. Latitude and longitude stay station points. They are not treated as city or state boundaries, and missing names are left missing.

```powershell
.\.venv\Scripts\python.exe main.py analyze
```

Aggregates are written as one Parquet file and one CSV per table under `data/processed/analytics/`. The report is `data/interim/analytics_report.json`. The raw files and the measurement and spike Parquet files are hashed before and after the job and are not rewritten.

## Recent snapshots from data.gov.in

The historical XKDR pipeline stays the source for spike detection and the city portal. A separate pair of commands reads the Government of India Open Government Data resource “Real time Air Quality Index from various locations.”

- Catalog: [data.gov.in/catalog/real-time-air-quality-index](https://www.data.gov.in/catalog/real-time-air-quality-index)
- Resource ID: `3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69`
- Endpoint: `https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69`
- Publisher: Central Pollution Control Board, under the Ministry of Environment, Forest and Climate Change
- Licence: National Data Sharing and Accessibility Policy (NDSAP) / Government Open Data License

Register at [data.gov.in](https://www.data.gov.in/), create an API key, and put it in the untracked `.env` file as `DATA_GOV_IN_API_KEY`. The commands never print or store that key.

```powershell
.\.venv\Scripts\python.exe main.py fetch-data-gov
.\.venv\Scripts\python.exe main.py prepare-data-gov
```

`fetch-data-gov` pages through the current table and writes each raw JSON page under `data/raw/data_gov_in/snapshots/`. Normalized rows go to `data/interim/data_gov_in/records/data.parquet`. A row is identified by state, city, station, pollutant, and `last_update`. Repeating a fetch does not store that row again. A later hour is appended. XKDR files under `data/raw/v1` are not rewritten.

`prepare-data-gov` reads only that normalized file with the shared Spark session. It parses `last_update` as `DD-MM-YYYY HH:MM:SS` in Asia/Kolkata, parses the reported minimum, maximum, and average, and checks coordinates. The API response inspected for this project has no measurement unit, so the prepared values keep the label “not documented by the API.” They are not called µg/m³, they are not filled with zero, and they are not passed through the rolling spike detector. No AQI field is created. The output is `data/processed/data_gov_in/`.

The resource is a current snapshot. A request for an older timestamp returned no rows. The portal draws a trend only after more than one distinct `last_update` has been stored for the same station and pollutant. That chart uses the API timestamp on the time axis and draws the reported minimum, maximum, and average as separate lines. A repeated fetch of the same hour stays one point. The Recent snapshots page says the data was retrieved. It does not describe the feed as a live stream.

`API last_update` is the timestamp reported by data.gov.in for that observation. The local retrieval time is when AEROSCOPE fetched the response. Repeating a fetch during the same source hour stores the raw JSON again and does not add another normalized observation.

### Automatic hourly collection

`scripts/run_data_gov_pipeline.ps1` runs `fetch-data-gov` and, only after that command exits 0, `prepare-data-gov`. It uses `.venv\Scripts\python.exe`, reads the existing `.env` entry, and does not print the API key. A second run waits outside the lock while the first run is active. Raw snapshots and older normalized rows are left in place. The dashboard does not call the API or Spark when the page refreshes; it reads `data/processed/data_gov_in/` and, when present, `logs/data_gov_automation/status.json`.

An hourly run does not guarantee a new observation. The API table often keeps the same `last_update` for every row until the publisher moves to the next hour.

Install the scheduled task only when you want it registered:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_data_gov_task.ps1
```

The task name is `AEROSCOPE_DataGov_Hourly`. It starts at the next local hour and repeats every hour. It runs as the current Windows user while that user is logged on, including when the AEROSCOPE browser tab is closed. It does not run after Windows sign-out, because the task does not store a password. A new run is ignored while the previous one is still active. Each run stops if it is still going after 50 minutes.

Check it:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\check_data_gov_task.ps1
```

Run it once by hand:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_data_gov_pipeline.ps1
```

Remove only that task:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\uninstall_data_gov_task.ps1
```

Logs are `logs/data_gov_automation/pipeline-*.log`, plus `status.json` for the latest result. Pipeline logs older than 14 days are deleted, and at most 400 pipeline logs are kept. `status.json`, raw snapshots, and normalized rows are not part of that cleanup.

If a run fails, open the newest `pipeline-*.log`. Exit code 3 means `.venv\Scripts\python.exe` is missing. Exit code 4 means `DATA_GOV_IN_API_KEY` is missing. A non-zero fetch exit means preparation did not start. A fetch exit of 0 followed by a non-zero preparation exit means Spark preparation failed. The log should contain `[REDACTED]` where a key would otherwise have been copied from command output. A Task Scheduler result of `3221225786` (`0xC000013A`) means the scheduled process was interrupted before it reported its own exit code. It is not a successful run. The install script does not pass `-WindowStyle Hidden`. Spark warnings on the Python error stream are saved in the pipeline log; they do not stop the run by themselves. Paths that contain spaces are passed to Python as single quoted arguments.

## Dashboard

The AEROSCOPE portal is a demonstration sign-in followed by a Timeline page. A Data coverage page in the sidebar lists which XKDR months are processed, partial, downloaded but outside the prepared CPCB series, or not downloaded. “Not downloaded” means the file is absent locally. It does not mean the publisher has no archive. The timeline offers the last 7, 30, or 90 days, the last year, or a custom start and end. It reads the prepared XKDR and data.gov.in tables and shows each source that has observations in that period. XKDR concentrations are in µg/m³. data.gov.in min, max, and average values have no documented unit, so the page does not average them with XKDR. A period with no rows states the stored date range for each source and does not substitute another period. Historical cities, City comparison, and Recent snapshots remain in the sidebar. The sign-in checks a PBKDF2-HMAC-SHA256 hash in `config/portal_users.json`. It is not production authentication: the demonstration password is published below, the session lives in the browser tab, and there is no account database.

`src/dashboard/app.py` reads the saved analytics tables, the city series, the analytics report, the annotated Parquet row count, and the spike-only rows. It does not rescore measurements. City daily and hourly charts come from `data/processed/city_portal`. The national `daily_by_pollutant` and `hourly_by_pollutant` tables are not used as city trends. Station rankings and the map use the selected city's saved station IDs over the full analyzed window. Stations without a city stay on the city page and are not given a city name. Stations without latitude and longitude are listed and are not placed on the map.

Build the city series once, then open the portal:

```powershell
.\.venv\Scripts\python.exe main.py build-city-series
.\.venv\Scripts\python.exe -m streamlit run src/dashboard/app.py
```

Demonstration account: `analyst@aeroscope.local` / `aeroscope-demo`.

## Performance

`evaluate-performance` times acquisition, inspection, preprocessing, spike detection, and analytics separately. Stage outputs are written to a temporary sandbox and deleted afterward. The saved files under `data/raw`, `data/interim`, and `data/processed` are hashed before and after the run and are not replaced. The JSON report is `data/interim/performance_report.json`. The readable report is `reports/performance.md`.

A file that is already present is skipped by `acquire`. That timing is the skip check, not the original download. No speedup is reported from a single local run.

```powershell
.\.venv\Scripts\python.exe main.py evaluate-performance
```
