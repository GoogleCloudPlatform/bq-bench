## BQ Bench

This tool allows you to do timed SQL query runs against Google BigQuery. It is
bundled with a set of TPC-DS power run queries.

All bundled queries are generated from the `dsqgen` (v3.2.0) tool by the [TPC](https://tpc.org), who is the copyright owner of the tools and query templates.

### Installation

```bash
git clone https://github.com/GoogleCloudPlatform/bq-bench
cd bq-bench
python3 -m venv venv
source venv/bin/activate

pip install -e .
```

### BigQuery client authentication
```
gcloud init
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
