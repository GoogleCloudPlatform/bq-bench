# Copyright 2025 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A tool for query benchmarking in BigQuery."""

import argparse
from collections.abc import Sequence
import csv
import dataclasses
import datetime
import logging
import os
import statistics
import time
from google.cloud import bigquery
import pyarrow
from pyarrow import csv as pyarrow_csv


@dataclasses.dataclass(frozen=True)
class Query:
  name: str
  sql: str


@dataclasses.dataclass(frozen=True)
class QueryExecution:
  """Represents a single execution of a query."""

  query: Query
  start_time: datetime.datetime
  duration_ms: int
  result_extraction_time_ms: int
  results: list[pyarrow.Table]
  iteration_index: int
  run_index: int
  run_mode: str
  job_id: str
  result_row_count: int
  result_size_bytes: int


def _load_query_ordering(query_dir: str) -> Sequence[str] | None:
  """Loads query ordering from the "query-order.txt" file in the given directory."""
  if not os.path.exists(os.path.join(query_dir, "query-order.txt")):
    return None
  with open(os.path.join(query_dir, "query-order.txt"), "r") as f:
    lines = f.read().splitlines()
    if not lines:
      return None
    return [entry.strip() for entry in lines[0].split(" ")]


def _load_queries(query_dir: str) -> Sequence[Query]:
  """Loads queries from a directory."""
  queries = []
  for query_file in os.listdir(query_dir):
    if not query_file.endswith(".sql"):
      continue
    with open(os.path.join(query_dir, query_file), "r") as f:
      query_sql = f.read()
      query_name = query_file.split(".")[0]
      queries.append(Query(name=query_name, sql=query_sql))
  query_name_ordering = _load_query_ordering(query_dir)
  if query_name_ordering is not None:
    logging.info(
        "Query ordering loaded for %d queries.", len(query_name_ordering)
    )
    query_map = {q.name: q for q in queries}
    return [query_map[name] for name in query_name_ordering]
  else:
    logging.info(
        "No query ordering found. Using sorted order for %d queries.",
        len(queries),
    )
    return sorted(queries, key=lambda q: q.name)


def _add_query_identification_comment(
    run_id: str,
    original_sql: str,
    query_name: str,
    iteration_index: int,
    run_index: int,
    run_mode: str,
    script_index: int,
) -> str:
  """Generates a query identification comment for the given query."""
  query_name_suffix = f"_{script_index}" if script_index > 0 else ""
  header_comment = (
      f"/* run_id={run_id}, run_mode={run_mode}, iter={iteration_index},"
      f" query={query_name}{query_name_suffix}, run_index={run_index} */"
  )
  return f"{header_comment}\n{original_sql}"


def _execute_query(
    run_id: str,
    client: bigquery.Client,
    query: Query,
    iteration_index: int,
    run_index: int,
    run_mode: str,
    store_results: bool,
) -> QueryExecution:
  """Executes a query and returns the results."""
  start_time = datetime.datetime.now()
  start_time_monotonic = time.monotonic()
  job_ids = []
  script_index = 0
  results: list[pyarrow.Table] = []
  result_extraction_time = 0.0
  num_rows = 0
  nbytes = 0
  for sql in query.sql.split(";"):
    sql = sql.strip()
    if not sql:
      continue
    result = client.query_and_wait(
        _add_query_identification_comment(
            run_id,
            sql,
            query.name,
            iteration_index,
            run_index,
            run_mode,
            script_index,
        ),
        page_size=1000,
    )
    job_id = f"{result.project}:{result.location}.{result.job_id}"
    job_ids.append(job_id)
    result_extraction_start_time = time.monotonic()
    table_data = result.to_arrow()
    num_rows += table_data.num_rows
    nbytes += table_data.nbytes
    if store_results:
      results.append(table_data)
    result_extraction_time += time.monotonic() - result_extraction_start_time
    script_index += 1
  end_time_monotonic = time.monotonic()
  query_execution = QueryExecution(
      query=query,
      start_time=start_time,
      duration_ms=(end_time_monotonic - start_time_monotonic) * 1000.0,
      result_extraction_time_ms=result_extraction_time * 1000.0,
      results=results,
      iteration_index=iteration_index,
      run_index=run_index,
      run_mode=run_mode,
      job_id=",".join(job_ids),
      result_row_count=num_rows,
      result_size_bytes=nbytes,
  )
  return query_execution


def _execute_queries(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference,
    queries: Sequence[Query],
    iteration_index: int,
    run_mode: str,
    store_results: bool,
) -> Sequence[QueryExecution]:
  """Executes queries and returns the results."""
  query_config = bigquery.job.QueryJobConfig(
      default_dataset=default_dataset,
      use_legacy_sql=False,
      use_query_cache=False,
  )
  client = bigquery.Client(
      project=project_id, default_query_job_config=query_config
  )
  query_executions = []
  for run_index, query in enumerate(queries, start=1):
    query_execution = _execute_query(
        run_id,
        client,
        query,
        iteration_index,
        run_index,
        run_mode,
        store_results,
    )
    logging.info(
        "Executed query: %s, iteration: %d, run index: %d, run mode: %s, client"
        " time: %.0fms, result extraction time: %.2fms [row count: %d, size: %d"
        " bytes]",
        query_execution.query.name,
        query_execution.iteration_index,
        query_execution.run_index,
        query_execution.run_mode,
        query_execution.duration_ms,
        query_execution.result_extraction_time_ms,
        query_execution.result_row_count,
        query_execution.result_size_bytes,
    )
    query_executions.append(query_execution)
  return query_executions


def _export_to_csv(
    query_executions: Sequence[QueryExecution], output_file: str
):
  """Exports query executions to a CSV file."""
  with open(output_file, "w", newline="") as csvfile:
    fieldnames = [
        "query",
        "start_time",
        "duration_ms",
        "result_extraction_time_ms",
        "result_row_count",
        "result_size_bytes",
        "iteration_index",
        "run_index",
        "job_id",
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for query_execution in query_executions:
      writer.writerow({
          "query": query_execution.query.name,
          "start_time": query_execution.start_time.isoformat(),
          "duration_ms": query_execution.duration_ms,
          "result_extraction_time_ms": (
              query_execution.result_extraction_time_ms
          ),
          "result_row_count": query_execution.result_row_count,
          "result_size_bytes": query_execution.result_size_bytes,
          "iteration_index": query_execution.iteration_index,
          "run_index": query_execution.run_index,
          "job_id": query_execution.job_id,
      })


def _export_report(
    run_id: str,
    query_executions: Sequence[QueryExecution],
    report_dir: str,
) -> None:
  """Exports query executions to a CSV file."""
  if not os.path.exists(report_dir):
    os.makedirs(report_dir)
  report_file = os.path.join(report_dir, f"{run_id}.csv")
  logging.info("Exporting report to: %s", report_file)
  _export_to_csv(
      query_executions,
      report_file,
  )


def _export_query_result_data(
    run_id: str,
    query_executions: Sequence[QueryExecution],
    results_base_dir: str,
) -> None:
  """Exports query results to a directory."""
  local_results_dir = os.path.join(results_base_dir, run_id)
  if not os.path.exists(local_results_dir):
    os.makedirs(local_results_dir)
  logging.info("Exporting query result data to: %s", local_results_dir)
  for query_execution in query_executions:
    for index, result in enumerate(query_execution.results):
      if len(query_execution.results) <= 26:
        script_suffix = (
            f"{chr(ord('a') + index)}"
            if len(query_execution.results) > 1
            else ""
        )
      else:
        script_suffix = f"_{index}"
      itr_suffix = (
          f"_{query_execution.iteration_index}"
          if query_execution.iteration_index > 1
          else ""
      )
      result_file = os.path.join(
          local_results_dir,
          f"{query_execution.query.name}{script_suffix}{itr_suffix}.csv",
      )
      pyarrow_csv.write_csv(result, result_file)


def _execute_warmup_iters(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference,
    queries: Sequence[Query],
    warmup_iters: int,
) -> float:
  """Executes warmup runs."""
  total_time = 0.0
  for i in range(1, warmup_iters + 1):
    logging.info("Running warmup execution %d out of %d...", i, warmup_iters)
    start_time = time.monotonic()
    _execute_queries(
        run_id,
        project_id,
        default_dataset,
        queries,
        i,
        run_mode="warmup",
        store_results=False,
    )
    end_time = time.monotonic()
    total_time += end_time - start_time
  return total_time


def _execute_test_iters(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference,
    queries: Sequence[Query],
    test_iters: int,
    store_results: bool,
) -> (Sequence[QueryExecution], [float]):
  """Executes test runs."""
  query_executions = []
  execution_times = []
  for i in range(1, test_iters + 1):
    logging.info("Running test execution %d out of %d...", i, test_iters)
    start_time = time.monotonic()
    query_executions.extend(
        _execute_queries(
            run_id,
            project_id,
            default_dataset,
            queries,
            i,
            run_mode="test",
            store_results=store_results,
        )
    )
    end_time = time.monotonic()
    execution_times.append(end_time - start_time)
  return (query_executions, execution_times)


def _run_queries(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference,
    query_dir: str,
    report_dir: str,
    query_results_dir: str,
    warmup_iters: int = 1,
    test_iters: int = 1,
) -> None:
  """Runs queries and exports results to a CSV file."""
  store_results = bool(query_results_dir)
  logging.info(
      "test project id: '%s'; default dataset: '%s'; query directory: '%s';"
      " report directory: '%s'; query results directory: '%s'",
      project_id,
      default_dataset,
      query_dir,
      report_dir,
      query_results_dir if store_results else "N/A",
  )
  queries = _load_queries(query_dir)
  total_warmup_time = _execute_warmup_iters(
      run_id, project_id, default_dataset, queries, warmup_iters
  )
  query_executions, test_exec_times = _execute_test_iters(
      run_id, project_id, default_dataset, queries, test_iters, store_results
  )
  logging.info("Total warmup run time: %.02fs", total_warmup_time)
  logging.info("Total test run time: %.02fs", sum(test_exec_times))
  logging.info(
      "Test run average times; mean: %.02fs median: %.02fs",
      statistics.mean(test_exec_times),
      statistics.median(test_exec_times),
  )
  logging.info("Run ID: %s", run_id)
  _export_report(run_id, query_executions, report_dir)
  if store_results:
    _export_query_result_data(run_id, query_executions, query_results_dir)


def main() -> None:
  """Main function to parse command-line arguments and run queries."""
  parser = argparse.ArgumentParser(description="Run BigQuery queries.")
  parser.add_argument(
      "--project_id",
      required=True,
      help="BigQuery project ID to run queries in.",
  )
  parser.add_argument(
      "--default_dataset",
      required=True,
      help=(
          "Default dataset name in the format of 'project.dataset' or excluding"
          " the project name if the dataset is in the same project as the query"
          " execution `project_id`."
      ),
  )
  parser.add_argument(
      "--query_dir",
      default="./queries/tpcds",
      help="Directory containing SQL query files.",
  )
  parser.add_argument(
      "--report_dir",
      default="./reports",
      help="Directory to store the report CSV files.",
  )
  parser.add_argument(
      "--query_results_dir",
      default="",
      help="Directory to store the query results CSV files.",
  )
  parser.add_argument(
      "--warmup_iters",
      type=int,
      default=1,
      help=(
          "Number of warmup iterations to execute before the test run"
          " [default=1]."
      ),
  )
  parser.add_argument(
      "--test_iters",
      type=int,
      default=1,
      help="Number of test iterations to execute [default=1].",
  )
  args = parser.parse_args()

  logging.basicConfig(
      level="INFO", format="%(asctime)s - %(levelname)s - %(message)s"
  )

  project_id = args.project_id
  default_dataset_parts = args.default_dataset.split(".")
  if len(default_dataset_parts) == 1:
    default_dataset_parts.insert(0, project_id)
  default_dataset = bigquery.dataset.DatasetReference(
      project=default_dataset_parts[0],
      dataset_id=default_dataset_parts[1],
  )

  run_id_prefix = (
      f"{default_dataset}"
      if default_dataset.project == project_id
      else f"{project_id}_{default_dataset}"
  )
  run_id = (
      f"{run_id_prefix}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
  )

  logging.info("Starting BQ Bench...")
  _run_queries(
      run_id,
      project_id,
      default_dataset,
      args.query_dir,
      args.report_dir,
      args.query_results_dir,
      args.warmup_iters,
      args.test_iters,
  )
  logging.info("Finished.")


if __name__ == "__main__":
  main()

