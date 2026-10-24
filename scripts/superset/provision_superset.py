"""Phase 6 - Cau hinh Superset: ket noi postgres-dw, tao dataset tu cac OLAP
view (Phase 4) + bang customer_segment (Phase 5.5), tao chart va 1 dashboard
tong hop, qua REST API cua Superset (khong thao tac UI thu cong).

Idempotent: kiem tra ton tai theo ten (database_name/table_name/slice_name/
dashboard_title) truoc khi tao - chay lai nhieu lan KHONG tao doi tuong trung,
chi cap nhat lai cau hinh (PUT) neu da ton tai.

KHONG doi du lieu DW (chi doc qua Postgres driver moi them cho container
Superset - xem docker-compose.yml). KHONG xoa dataset/chart/dashboard nao.

Yeu cau: da them `pip install psycopg2-binary` vao command cua service
`superset` trong docker-compose.yml va da `docker compose up -d superset` de
container co driver Postgres (xem docs/06_superset_dashboard.md).

Chay: uv run scripts/superset/provision_superset.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _client import SupersetClient  # noqa: E402

DATABASE_NAME = "Retail DW"

DATASETS = [
    "v_olap_sales_daily",
    "v_olap_sales_by_country",
    "v_olap_top10_products",
    "customer_segment",
    "v_olap_kpi_summary",
]

DASHBOARD_TITLE = "Online Retail II - Tong quan kinh doanh"


def metric(column: str, aggregate: str, label: str) -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label,
    }


def ensure_database(c: SupersetClient) -> int:
    existing = c.find_one("database", "database_name", DATABASE_NAME)
    if existing:
        print(f"[provision] Database '{DATABASE_NAME}' da ton tai (id={existing['id']}).")
        return existing["id"]

    uri = (
        f"postgresql+psycopg2://{os.environ['PG_DW_USER']}:{os.environ['PG_DW_PASSWORD']}"
        f"@postgres-dw:5432/{os.environ['PG_DW_DB']}"
    )
    resp = c.post(
        "/api/v1/database/",
        {"database_name": DATABASE_NAME, "sqlalchemy_uri": uri, "expose_in_sqllab": True},
    )
    db_id = resp["id"]
    print(f"[provision] Da tao database '{DATABASE_NAME}' (id={db_id}).")
    return db_id


def ensure_datasets(c: SupersetClient, db_id: int) -> dict[str, int]:
    ids: dict[str, int] = {}
    for table in DATASETS:
        existing = c.find_one("dataset", "table_name", table)
        if existing:
            ids[table] = existing["id"]
            print(f"[provision] Dataset '{table}' da ton tai (id={existing['id']}).")
            continue
        resp = c.post("/api/v1/dataset/", {"database": db_id, "schema": "public", "table_name": table})
        ids[table] = resp["id"]
        print(f"[provision] Da tao dataset '{table}' (id={resp['id']}).")
    return ids


def ensure_chart(c: SupersetClient, slice_name: str, viz_type: str, ds_id: int, params: dict) -> int:
    existing = c.find_one("chart", "slice_name", slice_name)
    body = {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
    }
    if existing:
        c.put(f"/api/v1/chart/{existing['id']}", body)
        print(f"[provision] Da cap nhat chart '{slice_name}' (id={existing['id']}).")
        return existing["id"]
    resp = c.post("/api/v1/chart/", body)
    chart_id = resp["id"]
    print(f"[provision] Da tao chart '{slice_name}' (id={chart_id}).")
    return chart_id


def build_charts(c: SupersetClient, ds: dict[str, int]) -> dict[str, int]:
    charts: dict[str, int] = {}

    charts["monthly_revenue"] = ensure_chart(
        c,
        "Doanh thu theo thang",
        "echarts_timeseries_line",
        ds["v_olap_sales_daily"],
        {
            "datasource": f"{ds['v_olap_sales_daily']}__table",
            "viz_type": "echarts_timeseries_line",
            "x_axis": "full_date",
            "time_grain_sqla": "P1M",
            "x_axis_sort_asc": True,
            "metrics": [metric("total_revenue", "SUM", "Tong doanh thu")],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 1000,
            "y_axis_format": "SMART_NUMBER",
            "color_scheme": "supersetColors",
            "contributionMode": None,
        },
    )

    rev_by_country_metric = metric("total_revenue", "SUM", "Tong doanh thu")
    charts["revenue_by_country"] = ensure_chart(
        c,
        "Doanh thu theo quoc gia",
        "dist_bar",
        ds["v_olap_sales_by_country"],
        {
            "datasource": f"{ds['v_olap_sales_by_country']}__table",
            "viz_type": "dist_bar",
            "metrics": [rev_by_country_metric],
            "groupby": ["country_name"],
            "adhoc_filters": [],
            "row_limit": 15,
            "order_desc": True,
            "timeseries_limit_metric": rev_by_country_metric,
            "color_scheme": "supersetColors",
            "y_axis_format": "SMART_NUMBER",
        },
    )

    rev_by_product_metric = metric("total_revenue", "SUM", "Tong doanh thu")
    charts["top10_products"] = ensure_chart(
        c,
        "Top 10 san pham theo doanh thu",
        "dist_bar",
        ds["v_olap_top10_products"],
        {
            "datasource": f"{ds['v_olap_top10_products']}__table",
            "viz_type": "dist_bar",
            "metrics": [rev_by_product_metric],
            "groupby": ["description"],
            "adhoc_filters": [],
            "row_limit": 10,
            "order_desc": True,
            "timeseries_limit_metric": rev_by_product_metric,
            "color_scheme": "supersetColors",
            "y_axis_format": "SMART_NUMBER",
        },
    )

    charts["segment_count"] = ensure_chart(
        c,
        "So khach hang theo phan khuc",
        "pie",
        ds["customer_segment"],
        {
            "datasource": f"{ds['customer_segment']}__table",
            "viz_type": "pie",
            "metric": metric("customer_key", "COUNT", "So khach hang"),
            "groupby": ["segment_name"],
            "adhoc_filters": [],
            "row_limit": 100,
            "color_scheme": "supersetColors",
            "label_type": "key_value_percent",
        },
    )

    charts["segment_rfm"] = ensure_chart(
        c,
        "RFM trung binh theo phan khuc",
        "dist_bar",
        ds["customer_segment"],
        {
            "datasource": f"{ds['customer_segment']}__table",
            "viz_type": "dist_bar",
            "metrics": [
                metric("monetary", "AVG", "Monetary trung binh"),
                metric("frequency", "AVG", "Frequency trung binh"),
            ],
            "groupby": ["segment_name"],
            "adhoc_filters": [],
            "row_limit": 10,
            "color_scheme": "supersetColors",
            "y_axis_format": "SMART_NUMBER",
        },
    )

    kpi_cols = [
        "total_revenue", "total_quantity", "fact_row_count", "invoice_count",
        "customer_count", "product_count", "country_count", "fact_rows_missing_customer",
    ]
    charts["kpi_summary"] = ensure_chart(
        c,
        "KPI tong quan kho du lieu",
        "table",
        ds["v_olap_kpi_summary"],
        {
            "datasource": f"{ds['v_olap_kpi_summary']}__table",
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": kpi_cols,
            "order_by_cols": [],
            "adhoc_filters": [],
            "row_limit": 10,
        },
    )

    return charts


def build_position_json(chart_ids: dict[str, int]) -> dict:
    def chart_component(key: str, slice_name: str, width: int) -> dict:
        return {
            "type": "CHART",
            "id": f"CHART-{key}",
            "children": [],
            "parents": [],
            "meta": {"chartId": chart_ids[key], "width": width, "height": 50, "sliceName": slice_name},
        }

    row1 = {
        "type": "ROW", "id": "ROW-1", "parents": ["GRID_ID"],
        "children": ["CHART-monthly_revenue"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    row2 = {
        "type": "ROW", "id": "ROW-2", "parents": ["GRID_ID"],
        "children": ["CHART-revenue_by_country", "CHART-top10_products"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    row3 = {
        "type": "ROW", "id": "ROW-3", "parents": ["GRID_ID"],
        "children": ["CHART-segment_count", "CHART-segment_rfm"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    row4 = {
        "type": "ROW", "id": "ROW-4", "parents": ["GRID_ID"],
        "children": ["CHART-kpi_summary"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }

    position_json: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"],
            "children": ["ROW-1", "ROW-2", "ROW-3", "ROW-4"],
        },
        "ROW-1": row1,
        "ROW-2": row2,
        "ROW-3": row3,
        "ROW-4": row4,
        "CHART-monthly_revenue": chart_component("monthly_revenue", "Doanh thu theo thang", 12),
        "CHART-revenue_by_country": chart_component("revenue_by_country", "Doanh thu theo quoc gia", 6),
        "CHART-top10_products": chart_component("top10_products", "Top 10 san pham theo doanh thu", 6),
        "CHART-segment_count": chart_component("segment_count", "So khach hang theo phan khuc", 6),
        "CHART-segment_rfm": chart_component("segment_rfm", "RFM trung binh theo phan khuc", 6),
        "CHART-kpi_summary": chart_component("kpi_summary", "KPI tong quan kho du lieu", 12),
        "DASHBOARD_VERSION_KEY_UNUSED": None,
    }
    position_json.pop("DASHBOARD_VERSION_KEY_UNUSED")
    return position_json


def ensure_dashboard(c: SupersetClient, chart_ids: dict[str, int], ds: dict[str, int]) -> int:
    existing = c.find_one("dashboard", "dashboard_title", DASHBOARD_TITLE)
    position_json = build_position_json(chart_ids)

    native_filters = [
        {
            "id": "NATIVE_FILTER-time",
            "type": "NATIVE_FILTER",
            "name": "Khoang thoi gian",
            "filterType": "filter_time",
            "targets": [{"datasetId": ds["v_olap_sales_daily"], "column": {"name": "full_date"}}],
            "defaultDataMask": {},
            "controlValues": {},
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        },
        {
            "id": "NATIVE_FILTER-country",
            "type": "NATIVE_FILTER",
            "name": "Quoc gia",
            "filterType": "filter_select",
            "targets": [{"datasetId": ds["v_olap_sales_by_country"], "column": {"name": "country_name"}}],
            "defaultDataMask": {},
            "controlValues": {"enableEmptyFilter": False, "multiSelect": True},
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        },
    ]
    json_metadata = json.dumps(
        {
            "native_filter_configuration": native_filters,
            "chart_configuration": {},
            "global_chart_configuration": {},
        }
    )

    body = {
        "dashboard_title": DASHBOARD_TITLE,
        "position_json": json.dumps(position_json),
        "json_metadata": json_metadata,
        "published": True,
    }

    if existing:
        dash_id = existing["id"]
        c.put(f"/api/v1/dashboard/{dash_id}", body)
        print(f"[provision] Da cap nhat dashboard '{DASHBOARD_TITLE}' (id={dash_id}).")
    else:
        resp = c.post("/api/v1/dashboard/", body)
        dash_id = resp["id"]
        print(f"[provision] Da tao dashboard '{DASHBOARD_TITLE}' (id={dash_id}).")

    for chart_id in chart_ids.values():
        chart = c.get(f"/api/v1/chart/{chart_id}")
        existing_dashboards = [d["id"] for d in chart["result"].get("dashboards", [])]
        if dash_id not in existing_dashboards:
            c.put(f"/api/v1/chart/{chart_id}", {"dashboards": existing_dashboards + [dash_id]})

    return dash_id


def main() -> None:
    c = SupersetClient()
    c.login()
    db_id = ensure_database(c)
    ds = ensure_datasets(c, db_id)
    chart_ids = build_charts(c, ds)
    dash_id = ensure_dashboard(c, chart_ids, ds)
    print(f"\n[provision] XONG. Dashboard id={dash_id}: "
          f"http://localhost:{os.environ.get('SUPERSET_PORT', '8088')}/superset/dashboard/{dash_id}/")


if __name__ == "__main__":
    main()
