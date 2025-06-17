## BQ Bench

This tool allows you to do timed SQL query runs against Google BigQuery. It is
bundled with a set of TPC-DS power run queries.

All bundled queries are generated from the `dsqgen` (v3.2.0) tool by the [TPC]
(https://tpc.org), who is the copyright owner of the tools and query templates.

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

More information on BigQuery client authentication can be found [here](https://cloud.google.com/bigquery/docs/authentication#client-libraries).

### Running

Usage:
```bash
bq-bench -h
usage: bq-bench [-h] --project_id PROJECT_ID --default_dataset DEFAULT_DATASET [--query_dir QUERY_DIR] [--report_dir REPORT_DIR] [--query_results_dir QUERY_RESULTS_DIR] [--warmup_iters WARMUP_ITERS] [--test_iters TEST_ITERS]

Run BigQuery queries.

options:
  -h, --help            show this help message and exit
  --project_id PROJECT_ID
                        BigQuery project ID to run queries in.
  --default_dataset DEFAULT_DATASET
                        Default dataset name in the format of 'project.dataset' or excluding the project name if the dataset is in the same project as the query execution `project_id`.
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
```

Example:
```bash
bq-bench --project_id=<test_project> --default_dataset=bigquery-public-data.tpc_ds_10t --query_dir=./third_party/queries/tpcds --report_dir=./reports
```
