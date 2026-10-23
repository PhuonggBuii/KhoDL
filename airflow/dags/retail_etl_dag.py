"""DAG tong the cho pipeline ETL Online Retail II.

Luong: start -> extract (API/File/Database song song) -> transform -> load -> validate -> end.

  - extract: goi lai scripts/ingest/*.py (Phase 1 + 2.2, da chay SUCCESS thuc trong
    container Airflow) qua volume mount "./scripts -> /opt/airflow/scripts" +
    mount mirror "./data", "./datalake", "./.env" (xem docker-compose.yml).
    Hostname tham so hoa qua FASTAPI_HOST/PG_SOURCE_HOST (Phase 2.2).
  - transform: goi scripts/transform/transform.py (Phase 2.3) - doc truc tiep
    data/raw/online_retail_II.xlsx (xem ly do trong file do), ap dung 5 quyet
    dinh nghiep vu da de xuat o docs/03_star_schema.md muc 6, xuat
    datalake/processed/<ngay>/{dim_*,fact_sales}.csv.
  - load: goi scripts/load/load.py (Phase 2.4) - UPSERT 4 dimension (khong xoa
    dong cu), TRUNCATE + INSERT lai fact_sales (idempotent, Phase 2.5).
  - validate: ket noi THUC toi postgres-dw, chay bo kiem tra Phase 3.3 (orphan
    FK, cong thuc revenue, don huy, NULL customer_key) - FAIL thuc neu co kiem
    tra khong dat, SKIP neu fact_sales rong (chua Load), KHONG bao thanh cong
    gia.
"""

from __future__ import annotations

import sys
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

SCRIPTS_DIR = "/opt/airflow/scripts"


def _run_module(subdir: str, module_name: str) -> None:
    """Goi lai scripts/<subdir>/<module_name>.py::run().

    Loi duoc de raise ro rang tu chinh script, khong nuot loi am tham o day.
    """
    path = f"{SCRIPTS_DIR}/{subdir}"
    if path not in sys.path:
        sys.path.insert(0, path)
    module = __import__(module_name)
    exit_code = module.run()
    if exit_code != 0:
        raise AirflowException(f"{module_name}.run() tra ve exit code {exit_code} (khac 0)")


def extract_api() -> None:
    _run_module("ingest", "ingest_api")


def extract_file() -> None:
    _run_module("ingest", "ingest_file")


def extract_database() -> None:
    _run_module("ingest", "ingest_database")


def transform_task() -> None:
    _run_module("transform", "transform")


def load_task() -> None:
    _run_module("load", "load")


def validate_dw() -> None:
    """Kiem tra thuc Phase 3.3 tren postgres-dw - KHONG bao thanh cong gia."""
    import os

    import psycopg2

    conn = psycopg2.connect(
        host="postgres-dw",
        port=5432,
        dbname=os.environ["PG_DW_DB"],
        user=os.environ["PG_DW_USER"],
        password=os.environ["PG_DW_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM fact_sales;")
            (n_fact,) = cur.fetchone()

        if n_fact == 0:
            raise AirflowSkipException(
                "fact_sales dang rong - Load chua chay hoac chua co du lieu. "
                "Khong co gi de validate thuc su."
            )

        checks: dict[str, int] = {}
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM fact_sales f LEFT JOIN dim_date d "
                "ON f.date_key = d.date_key WHERE d.date_key IS NULL;"
            )
            checks["orphan_date"] = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM fact_sales f LEFT JOIN dim_product d "
                "ON f.product_key = d.product_key WHERE d.product_key IS NULL;"
            )
            checks["orphan_product"] = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM fact_sales f LEFT JOIN dim_country d "
                "ON f.country_key = d.country_key WHERE d.country_key IS NULL;"
            )
            checks["orphan_country"] = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM fact_sales f LEFT JOIN dim_customer d "
                "ON f.customer_key = d.customer_key "
                "WHERE f.customer_key IS NOT NULL AND d.customer_key IS NULL;"
            )
            checks["orphan_customer"] = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM fact_sales WHERE invoice_no LIKE 'C%';")
            checks["cancelled_invoices_in_fact"] = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM fact_sales WHERE revenue <> round(quantity * unit_price, 2);"
            )
            checks["revenue_formula_mismatch"] = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM fact_sales WHERE customer_key IS NULL;")
            n_customer_null = cur.fetchone()[0]

        print(f"[validate] fact_sales = {n_fact:,} dong. Ket qua kiem tra: {checks}. "
              f"customer_key NULL (hop le, khong phai loi) = {n_customer_null:,}.")

        failures = {k: v for k, v in checks.items() if v != 0}
        if failures:
            raise AirflowException(f"Validate THAT BAI, cac kiem tra khong dat: {failures}")

        print("[validate] TAT CA kiem tra Phase 3.3 PASS: khong orphan FK, khong don huy "
              "trong fact, revenue dung cong thuc.")
    finally:
        conn.close()


with DAG(
    dag_id="retail_online_ii_etl",
    description=(
        "Phase 2 - ETL Online Retail II: Extract (API/File/DB) -> Transform -> Load -> "
        "Validate. Tat ca task da chay thuc (Phase 2.2-2.5, 3.3)."
    ),
    schedule=None,  # Phase 2.1: chua co lich tu dong, trigger thu cong khi can test.
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["phase2", "etl", "retail"],
    default_args={"owner": "khodl", "retries": 0},
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="extract") as extract_group:
        PythonOperator(task_id="extract_api", python_callable=extract_api)
        PythonOperator(task_id="extract_file", python_callable=extract_file)
        PythonOperator(task_id="extract_database", python_callable=extract_database)

    transform = PythonOperator(task_id="transform", python_callable=transform_task)
    load = PythonOperator(task_id="load", python_callable=load_task)
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_dw,
        trigger_rule="all_done",  # chay du upstream bi skip, de kiem tra thuc trang DW
    )
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    start >> extract_group >> transform >> load >> validate >> end
