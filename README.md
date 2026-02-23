## BQ Bench

This tool allows you to do timed SQL query runs against Google BigQuery. It is
bundled with a set of TPC-DS power run queries.

All bundled queries are generated from the `dsqgen` (v3.2.0) tool by the [TPC](https://tpc.org), who is the copyright owner of the tools and query templates.

### Prerequisites

* [Python](https://www.python.org/downloads/) v3.4 or later.
* Required IAM user roles:
  - [BigQuery Job User](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser) (roles/bigquery.jobUser)
  - [BigQuery Read Session User](https://cloud.google.com/bigquery/docs/access-control#bigquery.readSessionUser) (roles/bigquery.readSessionUser)

### Installation

* Check-out the `bq-bench` code:

```bash
git clone https://github.com/GoogleCloudPlatform/bq-bench
cd bq-bench
```

* Optional: create an isolated Python virtual environment (https://docs.python.org/3/library/venv.html):
```bash
python3 -m venv venv
source venv/bin/activate
```

* Build the code and install `bqbench`:
```bash
pip install -e .
```

### BigQuery client authentication

Initialize gcloud command-line tooling:
```bash
gcloud init
```

Acquire new user credentials:
```bash
gcloud auth application-default login
```

If Google spreadsheet report export is used, the authentication should be done with the following scopes as well:
```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive
```

Also, for Google spreadsheet report export, a quota project should be configured in the following manner:
```bash
gcloud config set billing/quota_project <project>
```

More information on BigQuery client authentication can be found [here](https://cloud.google.com/bigquery/docs/authentication#client-libraries).

### Running

Usage:
```bash
bq-bench -h
usage: bq-bench [-h] --project_id PROJECT_ID --default_dataset DEFAULT_DATASET [--query_dir QUERY_DIR] [--report_dir REPORT_DIR]
                [--query_results_dir QUERY_RESULTS_DIR] [--warmup_iters WARMUP_ITERS] [--test_iters TEST_ITERS] [--interleave_query_iterations]
                [--skip_reading_results] [--export_to_sheets] [--export_report_verbose]

Run BigQuery queries.

options:
  -h, --help            show this help message and exit
  --project_id PROJECT_ID
                        BigQuery project ID to run queries in.
  --default_dataset DEFAULT_DATASET
                        Default dataset name in the format of 'project.dataset' or excluding the project name if the dataset is in the same project as the query
                        execution `project_id`.
  --query_dir QUERY_DIR
                        Directory containing SQL query files.
  --report_dir REPORT_DIR
                        Directory to store the report CSV files.
  --query_results_dir QUERY_RESULTS_DIR
                        Directory to store the query results CSV files.
  --warmup_iters WARMUP_ITERS
                        Number of warmup iterations to execute before the test run [default=1].
  --test_iters TEST_ITERS
                        Number of test iterations to execute [default=1].
  --interleave_query_iterations
                        If query iterations should be interleaved or executed in sequence; i.e. interleaved: query1-iter1, query2-iter1, ... query1-iter2,
                        query2-iter2, ... - sequencial: query1-iter1, query1-iter2, ... query2-iter1, query2-iter2, ... [default=false (sequential)]
  --skip_reading_results
                        If true, skip reading the results of the queries [default=false].
  --export_to_sheets    If true, export the report to a new Google Sheet.
  --export_report_verbose
                        If true, export the report with all query executions (not only the median) [default=false].
```
Examples:
```bash
bq-bench --project_id=<test_project> --default_dataset=bigquery-public-data.tpc_ds_10t --query_dir=./third_party/queries/tpcds --report_dir=./reports
```
```bash
bq-bench --export_to_sheets --skip_reading_results --test_iters=3 --project_id=<test_project> --default_dataset=bigquery-public-data.tpc_ds_1g --query_dir=./third_party/queries/tpcds --report_dir=./reports
```

### Executed Query Analysis

The information on the executed queries are available in a CSV file in the reporting directory (if provided). This will contain the per-query statistics, and their job IDs. The executed SQL text contains a comment header, which can be used to generally parse or filter the queries that has been executed in the test project.

Following is the general format of the comment header: ```/* run_id=<run_id>, run_mode=<run_mode>, iter=<iter>, query=<query>, run_index=<run_index> */```

* `run_id`: A unique string identifier for the benchmark run
* `run_mode`: Either "warmup" or "test", representing the warmup and the test iterations respectively
* `iter`: The iteration index of the current query execution iteration. This is a 1-based number (value starts from 1).
* `query`: The name of the query that is executed. This is the name of the file in the query directory (exluding the file extension)
* `run_index`: The position of the query currently being executed. This is a 1-based number.

Sample query for looking up test query information for a specific run from INFORMATION_SCHEMA:

```sql
SELECT
  *
FROM
  `<project>`.`region-<region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND query LIKE "%run_id=<run_id>, run_mode=test";
```

