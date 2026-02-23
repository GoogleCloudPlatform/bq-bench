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
import collections
from collections.abc import Sequence
import csv
import dataclasses
import datetime
import logging
import os
import statistics
import time

import google.auth
from google.auth import exceptions as google_auth_exceptions
from google.cloud import bigquery
import gspread
from gspread import exceptions as gspread_exceptions
import pyarrow
from pyarrow import csv as pyarrow_csv


@dataclasses.dataclass(frozen=True)
class Query:
  name: str
  sql: str


@dataclasses.dataclass
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
  total_slot_millis: int


@dataclasses.dataclass(frozen=True)
class QueryExecutionReportData:
  """Represents a query execution report data."""

  all_query_executions: Sequence[QueryExecution]
  median_runtime_query_executions: Sequence[QueryExecution]


MAX_PAGE_SIZE = 1000


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
    query_execution: QueryExecution,
    store_results: bool,
    skip_reading_results: bool,
) -> None:
  """Executes a query and returns the results."""
  start_time = datetime.datetime.now()
  start_time_monotonic = time.monotonic()
  job_ids = []
  script_index = 0
  results: list[pyarrow.Table] = []
  result_extraction_time = 0.0
  num_rows = 0
  nbytes = 0
  total_slot_millis = 0
  for sql in query_execution.query.sql.split(";"):
    sql = sql.strip()
    if not sql:
      continue
    result = client.query_and_wait(
        _add_query_identification_comment(
            run_id,
            sql,
            query_execution.query.name,
            query_execution.iteration_index,
            query_execution.run_index,
            query_execution.run_mode,
            script_index,
        ),
        page_size=None if skip_reading_results else MAX_PAGE_SIZE,
        max_results=0 if skip_reading_results else None,
    )
    job_id = f"{result.project}:{result.location}.{result.job_id}"
    job_ids.append(job_id)
    total_slot_millis += result.slot_millis
    if not skip_reading_results:
      result_extraction_start_time = time.monotonic()
      table_data = result.to_arrow()
      num_rows += table_data.num_rows
      nbytes += table_data.nbytes
      if store_results:
        results.append(table_data)
      result_extraction_time += time.monotonic() - result_extraction_start_time
    script_index += 1
  end_time_monotonic = time.monotonic()
  query_execution.start_time = start_time
  query_execution.duration_ms = round(
      (end_time_monotonic - start_time_monotonic) * 1000.0
  )
  query_execution.result_extraction_time_ms = round(
      result_extraction_time * 1000.0
  )
  query_execution.results = results
  query_execution.job_id = ",".join(job_ids)
  query_execution.result_row_count = num_rows
  query_execution.result_size_bytes = nbytes
  query_execution.total_slot_millis = total_slot_millis


def _generate_query_executions(
    run_mode: str,
    iteration_count: int,
    queries: Sequence[Query],
    interleave_query_iterations: bool,
) -> Sequence[QueryExecution]:
  """Generates query executions for the given query/strategy."""
  query_executions = []
  if interleave_query_iterations:
    for iteration_index in range(1, iteration_count + 1):
      for run_index, query in enumerate(queries, start=1):
        query_execution = QueryExecution(
            query=query,
            start_time=None,
            duration_ms=0,
            result_extraction_time_ms=0,
            results=[],
            iteration_index=iteration_index,
            run_index=run_index,
            run_mode=run_mode,
            job_id=None,
            result_row_count=0,
            result_size_bytes=0,
            total_slot_millis=0,
        )
        query_executions.append(query_execution)
  else:
    for run_index, query in enumerate(queries, start=1):
      for iteration_index in range(1, iteration_count + 1):
        query_execution = QueryExecution(
            query=query,
            start_time=None,
            duration_ms=0,
            result_extraction_time_ms=0,
            results=[],
            iteration_index=iteration_index,
            run_index=run_index,
            run_mode=run_mode,
            job_id=None,
            result_row_count=0,
            result_size_bytes=0,
            total_slot_millis=0,
        )
        query_executions.append(query_execution)
  return query_executions


def _execute_queries(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference,
    queries: Sequence[Query],
    iteration_count: int,
    run_mode: str,
    store_results: bool,
    interleave_query_iterations: bool,
    skip_reading_results: bool,
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
  query_executions = _generate_query_executions(
      run_mode, iteration_count, queries, interleave_query_iterations
  )
  for query_execution in query_executions:
    _execute_query(
        run_id,
        client,
        query_execution,
        store_results,
        skip_reading_results,
    )
    logging.info(
        "Executed query: %s, iteration: %d, run index: %d, run mode: %s, client"
        " time: %dms, result extraction time: %dms [row count: %d, size: %d"
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
  return query_executions


def _spreadsheet_row_from_execution(
    qe: QueryExecution, include_sql: bool = False
) -> dict[str, object]:
  """Converts a QueryExecution object to a dictionary for spreadsheet export."""
  return {
      "query": qe.query.name,
      "start_time": qe.start_time.isoformat(),
      "duration_ms": qe.duration_ms,
      "result_extraction_time_ms": qe.result_extraction_time_ms,
      "result_row_count": qe.result_row_count,
      "result_size_bytes": qe.result_size_bytes,
      "iteration_index": qe.iteration_index,
      "run_index": qe.run_index,
      "job_id": qe.job_id,
      "total_slot_millis": qe.total_slot_millis,
  } | ({"sql": qe.query.sql} if include_sql else {})


def _export_query_execution_details_to_csv(
    query_executions: Sequence[QueryExecution], output_file: str
):
  """Exports query executions to a CSV file."""
  if not query_executions:
    return
  with open(output_file, "w", newline="") as csvfile:
    fieldnames = list(
        _spreadsheet_row_from_execution(query_executions[0]).keys()
    )
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for query_execution in query_executions:
      writer.writerow(_spreadsheet_row_from_execution(query_execution))


def _select_median_runtime_query_executions(
    query_executions: Sequence[QueryExecution],
) -> Sequence[QueryExecution]:
  """Selects the median runtime query executions for each query."""
  query_executions_by_name = collections.defaultdict(list)
  for qe in query_executions:
    query_executions_by_name[qe.query.name].append(qe)
  median_query_executions = []
  for query_name in sorted(query_executions_by_name.keys()):
    query_execution_list = query_executions_by_name[query_name]
    query_execution_list.sort(key=lambda qe: qe.duration_ms)
    median_query_executions.append(
        query_execution_list[len(query_execution_list) // 2]
    )
  return median_query_executions


def _update_worksheet_with_executions(
    worksheet: gspread.Worksheet,
    query_executions: Sequence[QueryExecution],
    include_sql: bool,
):
  """Updates a worksheet with query execution data."""
  rows = [
      _spreadsheet_row_from_execution(qe, include_sql=include_sql)
      for qe in query_executions
  ]
  header = list(rows[0].keys())
  data = [header] + [[row[col] for col in header] for row in rows]
  worksheet.update(data)
  worksheet.freeze(rows=1)


def _export_to_google_sheet(
    run_id: str,
    report_data: QueryExecutionReportData,
    export_report_verbose: bool,
) -> None:
  """Exports query executions to a new Google Sheet."""
  try:
    creds, _ = google.auth.default()
    gc = gspread.authorize(creds)
    spreadsheet = gc.create(f"BQ Bench Report: {run_id}")
    logging.info("Exporting to Google Sheet: %s", spreadsheet.url)
    median_ws = spreadsheet.add_worksheet(
        title="Median Runtime Query Executions", rows=1, cols=1
    )
    _update_worksheet_with_executions(
        median_ws, report_data.median_runtime_query_executions, True
    )
    if export_report_verbose:
      all_ws = spreadsheet.add_worksheet(
          title="All Query Executions", rows=1, cols=1
      )
      _update_worksheet_with_executions(
          all_ws, report_data.all_query_executions, False
      )
    spreadsheet.del_worksheet(spreadsheet.sheet1)
  except (
      gspread_exceptions.APIError,
      google_auth_exceptions.DefaultCredentialsError,
  ) as e:
    logging.error("Failed to export to Google Sheet: %s", e)


def _export_csv_reports(
    run_id: str,
    report_data: QueryExecutionReportData,
    report_dir: str,
    export_report_verbose: bool,
) -> None:
  """Exports query executions to a CSV file."""
  run_dir = os.path.join(report_dir, run_id)
  if not os.path.exists(run_dir):
    os.makedirs(run_dir)
  logging.info("Exporting reports to: %s", run_dir)
  if export_report_verbose:
    _export_query_execution_details_to_csv(
        report_data.all_query_executions,
        os.path.join(run_dir, "all_query_executions.csv"),
    )
  _export_query_execution_details_to_csv(
      report_data.median_runtime_query_executions,
      os.path.join(run_dir, "median_runtime_query_executions.csv"),
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
    interleave_query_iterations: bool,
    skip_reading_results: bool,
) -> Sequence[QueryExecution]:
  """Executes warmup runs."""
  query_executions = _execute_queries(
      run_id,
      project_id,
      default_dataset,
      queries,
      warmup_iters,
      "warmup",
      False,
      interleave_query_iterations,
      skip_reading_results,
  )
  return query_executions


def _execute_test_iters(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference,
    queries: Sequence[Query],
    test_iters: int,
    store_results: bool,
    interleave_query_iterations: bool,
    skip_reading_results: bool,
) -> Sequence[QueryExecution]:
  """Executes test runs."""
  query_executions = _execute_queries(
      run_id,
      project_id,
      default_dataset,
      queries,
      test_iters,
      "test",
      store_results,
      interleave_query_iterations,
      skip_reading_results,
  )
  return query_executions


def _calculate_statistics(
    query_executions: Sequence[QueryExecution],
    interleave_query_iterations: bool,
) -> tuple[float, float]:
  """Calculates statistics for the query executions."""
  if interleave_query_iterations:
    query_durations = collections.defaultdict(list)
    for qe in query_executions:
      query_durations[qe.query.name].append(qe.duration_ms)
    query_means = [statistics.mean(v) for v in query_durations.values()]
    query_medians = [statistics.median(v) for v in query_durations.values()]
    return (sum(query_means), sum(query_medians))
  else:
    iteration_durations = collections.defaultdict(list)
    for qe in query_executions:
      iteration_durations[qe.iteration_index].append(qe.duration_ms)
    iteration_sums = [sum(v) for v in iteration_durations.values()]
    return (statistics.mean(iteration_sums), statistics.median(iteration_sums))


def _generate_report_data(
    query_executions: Sequence[QueryExecution],
) -> QueryExecutionReportData:
  return QueryExecutionReportData(
      all_query_executions=query_executions,
      median_runtime_query_executions=_select_median_runtime_query_executions(
          query_executions
      ),
  )


def _process_results(
    run_id: str,
    report_dir: str,
    query_results_dir: str,
    store_results: bool,
    warmup_query_executions: Sequence[QueryExecution],
    test_query_executions: Sequence[QueryExecution],
    interleave_query_iterations: bool,
    export_to_sheets: bool,
    export_report_verbose: bool,
):
  """Processes the results of the query executions."""
  total_warmup_time = (
      sum(qe.duration_ms for qe in warmup_query_executions) / 1000.0
  )
  total_test_time = sum(qe.duration_ms for qe in test_query_executions) / 1000.0
  logging.info("Total warmup run time: %.02fs", total_warmup_time)
  logging.info("Total test run time: %.02fs", total_test_time)
  mean_time, median_time = _calculate_statistics(
      test_query_executions, interleave_query_iterations
  )
  logging.info(
      "Test run average times; mean-based: %.02fs median-based: %.02fs",
      mean_time,
      median_time,
  )
  logging.info("Run ID: %s", run_id)
  report_data = _generate_report_data(test_query_executions)
  _export_csv_reports(run_id, report_data, report_dir, export_report_verbose)
  if export_to_sheets:
    _export_to_google_sheet(run_id, report_data, export_report_verbose)
  if store_results:
    _export_query_result_data(run_id, test_query_executions, query_results_dir)


def _run_queries(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference,
    query_dir: str,
    report_dir: str,
    query_results_dir: str,
    warmup_iters: int = 1,
    test_iters: int = 1,
    interleave_query_iterations: bool = False,
    skip_reading_results: bool = False,
    export_to_sheets: bool = False,
    export_report_verbose: bool = False,
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
  warmup_query_executions = _execute_warmup_iters(
      run_id,
      project_id,
      default_dataset,
      queries,
      warmup_iters,
      interleave_query_iterations,
      skip_reading_results,
  )
  test_query_executions = _execute_test_iters(
      run_id,
      project_id,
      default_dataset,
      queries,
      test_iters,
      store_results,
      interleave_query_iterations,
      skip_reading_results,
  )
  _process_results(
      run_id,
      report_dir,
      query_results_dir,
      store_results,
      warmup_query_executions,
      test_query_executions,
      interleave_query_iterations,
      export_to_sheets,
      export_report_verbose,
  )


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
  parser.add_argument(
      "--interleave_query_iterations",
      action="store_true",
      help=(
          "If query iterations should be interleaved or executed in sequence;"
          " i.e. interleaved: query1-iter1, query2-iter1, ... query1-iter2,"
          " query2-iter2, ...  - sequencial: query1-iter1, query1-iter2, ..."
          " query2-iter1, query2-iter2, ... [default=false (sequential)]"
      ),
  )
  parser.add_argument(
      "--skip_reading_results",
      action="store_true",
      help="If true, skip reading the results of the queries [default=false].",
  )
  parser.add_argument(
      "--export_to_sheets",
      action="store_true",
      help="If true, export the report to a new Google Sheet.",
  )
  parser.add_argument(
      "--export_report_verbose",
      action="store_true",
      help=(
          "If true, export the report with all query executions (not only the"
          " median) [default=false]."
      ),
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
      args.interleave_query_iterations,
      args.skip_reading_results,
      args.export_to_sheets,
      args.export_report_verbose,
  )
  logging.info("Finished.")


if __name__ == "__main__":
  main()

