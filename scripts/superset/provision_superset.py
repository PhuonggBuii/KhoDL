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

# Phase 8.1 - dashboard rieng cho ket qua Apriori (market_basket_rules), tach
# biet voi dashboard tong quan kinh doanh o tren - KHONG dong vao dashboard do.
APRIORI_DATASET_TABLE = "market_basket_rules"
APRIORI_DASHBOARD_TITLE = "Apriori - Luat ket hop san pham"
APRIORI_RULE_COLUMNS = ["antecedent_items", "consequent_items", "support", "confidence", "lift"]


def metric(column: str, aggregate: str, label: str) -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label,
    }


def sql_metric(sql_expression: str, label: str) -> dict:
    """Adhoc metric bang bieu thuc SQL tuy y (vd COUNT(CASE WHEN ... THEN 1 END)) -
    dung cho KPI "so luat co lift > 1" ma khong co san 1 cot/aggregate don le."""
    return {"expressionType": "SQL", "sqlExpression": sql_expression, "label": label}


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


def ensure_chart(
    c: SupersetClient, slice_name: str, viz_type: str, ds_id: int, params: dict, lookup_name: str | None = None
) -> int:
    """lookup_name: ten CU dung de TIM chart co san (vd khi doi ten/doi viz_type
    1 chart da ton tai) - mac dinh = slice_name (truong hop binh thuong, khong
    doi ten). Dung de SUA 1 chart tai cho (PUT), KHONG tao chart trung ten moi."""
    existing = c.find_one("chart", "slice_name", lookup_name or slice_name)
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


def ensure_apriori_dataset(c: SupersetClient, db_id: int) -> int:
    existing = c.find_one("dataset", "table_name", APRIORI_DATASET_TABLE)
    if existing:
        print(f"[provision] Dataset '{APRIORI_DATASET_TABLE}' da ton tai (id={existing['id']}).")
        return existing["id"]
    resp = c.post("/api/v1/dataset/", {"database": db_id, "schema": "public", "table_name": APRIORI_DATASET_TABLE})
    print(f"[provision] Da tao dataset '{APRIORI_DATASET_TABLE}' (id={resp['id']}).")
    return resp["id"]


def _rule_metrics() -> list[dict]:
    return [
        metric("support", "MAX", "Support"),
        metric("confidence", "MAX", "Confidence"),
        metric("lift", "MAX", "Lift"),
    ]


def _top_n_table_params(datasource: str, order_metric_label: str, order_desc: bool, row_limit: int) -> dict:
    """Bang top-N theo 1 metric, dung dung co che da xac minh HOAT DONG trong
    dashboard nay (groupby + timeseries_limit_metric + order_desc - giong het
    cac chart dist_bar Phase 6 da on dinh), KHONG dung "order_by_cols" (xem
    ghi chu sua loi ben duoi build_apriori_charts)."""
    order_metric = next(m for m in _rule_metrics() if m["label"] == order_metric_label)
    return {
        "datasource": datasource,
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": ["antecedent_items", "consequent_items"],
        "metrics": _rule_metrics(),
        "timeseries_limit_metric": order_metric,
        "order_desc": order_desc,
        "adhoc_filters": [],
        "row_limit": row_limit,
    }


def build_apriori_charts(c: SupersetClient, ds_id: int) -> dict[str, int]:
    """5 chart cho Phase 8.1 (KPI, top-10 theo Lift, top-10 theo Confidence,
    phan bo theo Support, bang chi tiet toan bo luat) tren dataset
    market_basket_rules.

    SUA LOI (2026-10-28, sau khi nguoi dung bao chi thay chart KPI hien thi,
    4 chart con lai KHONG hien ra trong dashboard): da kiem tra ky bang
    REST API - dataset/chart/dashboard association, position_json deu dung
    (xem docs/08_apriori_mba.md muc 12 de biet chi tiet qua trinh dieu tra).
    Nguyen nhan xac dinh duoc qua so sanh truc tiep voi 1 chart "table" cu DA
    HOAT DONG on dinh (chart "KPI tong quan kho du lieu", Phase 6): chart do
    dung "query_mode": "raw" + "order_by_cols": [] (RONG). 3 chart loi
    (apriori_top_lift/top_confidence/detail) la "table" raw voi
    "order_by_cols" KHONG RONG - day la diem khac biet duy nhat so voi chart
    da hoat dong. Chart scatter (echarts_timeseries_scatter) cung khong hien
    - vi khong the xac minh rendering qua API (chi xac minh duoc tang du
    lieu, khong xac minh duoc tang frontend), thay bang dang "table"
    (aggregate) cung co che da xac nhan on dinh, thay vi tiep tuc dung 1
    viz_type chua the kiem chung duoc trong moi truong nay.

    Sua: 3 bang top-N chuyen sang "query_mode": "aggregate" +
    "timeseries_limit_metric" + "order_desc" - CHINH LA co che sap xep da
    dung thanh cong trong cac chart dist_bar on dinh cua Phase 6
    (vd "Doanh thu theo quoc gia", "Top 10 san pham theo doanh thu") - khong
    phai co che moi, tu suy doan.
    """
    charts: dict[str, int] = {}
    datasource = f"{ds_id}__table"

    charts["apriori_kpi"] = ensure_chart(
        c,
        "Apriori - KPI tong quan",
        "table",
        ds_id,
        {
            "datasource": datasource,
            "viz_type": "table",
            "query_mode": "aggregate",
            "metrics": [
                metric("rule_id", "COUNT", "Tong so luat"),
                sql_metric("COUNT(CASE WHEN lift > 1 THEN 1 END)", "So luat co lift > 1"),
                metric("confidence", "AVG", "Confidence trung binh"),
                metric("lift", "AVG", "Lift trung binh"),
            ],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 1,
        },
    )

    charts["apriori_top_lift"] = ensure_chart(
        c,
        "Apriori - Top 10 luat theo Lift",
        "table",
        ds_id,
        _top_n_table_params(datasource, "Lift", order_desc=True, row_limit=10),
    )

    charts["apriori_top_confidence"] = ensure_chart(
        c,
        "Apriori - Top 10 luat theo Confidence",
        "table",
        ds_id,
        _top_n_table_params(datasource, "Confidence", order_desc=True, row_limit=10),
    )

    # Thay the chart Scatter (echarts_timeseries_scatter) - xem docstring ham
    # nay. Dung lai CUNG 1 chart id (lookup_name = ten CU) de KHONG tao chart
    # trung, chi doi noi dung + doi ten cho dung voi dang bang moi.
    charts["apriori_support_distribution"] = ensure_chart(
        c,
        "Apriori - Phan bo Support, Confidence, Lift theo luat",
        "table",
        ds_id,
        _top_n_table_params(datasource, "Lift", order_desc=False, row_limit=200),
        lookup_name="Apriori - Scatter Support vs Confidence",
    )

    charts["apriori_detail"] = ensure_chart(
        c,
        "Apriori - Bang chi tiet luat ket hop",
        "table",
        ds_id,
        _top_n_table_params(datasource, "Lift", order_desc=True, row_limit=200),
    )

    return charts


def build_apriori_position_json(chart_ids: dict[str, int]) -> dict:
    def chart_component(key: str, slice_name: str, width: int, height: int = 50) -> dict:
        return {
            "type": "CHART",
            "id": f"CHART-{key}",
            "children": [],
            "parents": [],
            "meta": {"chartId": chart_ids[key], "width": width, "height": height, "sliceName": slice_name},
        }

    row1 = {
        "type": "ROW", "id": "A-ROW-1", "parents": ["A-GRID_ID"],
        "children": ["CHART-apriori_kpi"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    row2 = {
        "type": "ROW", "id": "A-ROW-2", "parents": ["A-GRID_ID"],
        "children": ["CHART-apriori_top_lift", "CHART-apriori_top_confidence"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    row3 = {
        "type": "ROW", "id": "A-ROW-3", "parents": ["A-GRID_ID"],
        "children": ["CHART-apriori_support_distribution"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    row4 = {
        "type": "ROW", "id": "A-ROW-4", "parents": ["A-GRID_ID"],
        "children": ["CHART-apriori_detail"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }

    return {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["A-GRID_ID"]},
        "A-GRID_ID": {
            "type": "GRID", "id": "A-GRID_ID", "parents": ["ROOT_ID"],
            "children": ["A-ROW-1", "A-ROW-2", "A-ROW-3", "A-ROW-4"],
        },
        "A-ROW-1": row1,
        "A-ROW-2": row2,
        "A-ROW-3": row3,
        "A-ROW-4": row4,
        # height=50 (mac dinh chuan, giong het dashboard Phase 6) cho TAT CA
        # chart - khong con dung height=25 rieng cho KPI nhu truoc (da loai
        # bien so nay khi dieu tra loi, xem docstring build_apriori_charts).
        "CHART-apriori_kpi": chart_component("apriori_kpi", "Apriori - KPI tong quan", 12),
        "CHART-apriori_top_lift": chart_component("apriori_top_lift", "Apriori - Top 10 luat theo Lift", 6),
        "CHART-apriori_top_confidence": chart_component(
            "apriori_top_confidence", "Apriori - Top 10 luat theo Confidence", 6
        ),
        "CHART-apriori_support_distribution": chart_component(
            "apriori_support_distribution", "Apriori - Phan bo Support, Confidence, Lift theo luat", 12
        ),
        "CHART-apriori_detail": chart_component("apriori_detail", "Apriori - Bang chi tiet luat ket hop", 12, 60),
    }


def ensure_apriori_dashboard(c: SupersetClient, chart_ids: dict[str, int]) -> int:
    existing = c.find_one("dashboard", "dashboard_title", APRIORI_DASHBOARD_TITLE)
    position_json = build_apriori_position_json(chart_ids)
    json_metadata = json.dumps(
        {"native_filter_configuration": [], "chart_configuration": {}, "global_chart_configuration": {}}
    )

    body = {
        "dashboard_title": APRIORI_DASHBOARD_TITLE,
        "position_json": json.dumps(position_json),
        "json_metadata": json_metadata,
        "published": True,
    }

    if existing:
        dash_id = existing["id"]
        c.put(f"/api/v1/dashboard/{dash_id}", body)
        print(f"[provision] Da cap nhat dashboard '{APRIORI_DASHBOARD_TITLE}' (id={dash_id}).")
    else:
        resp = c.post("/api/v1/dashboard/", body)
        dash_id = resp["id"]
        print(f"[provision] Da tao dashboard '{APRIORI_DASHBOARD_TITLE}' (id={dash_id}).")

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

    apriori_ds_id = ensure_apriori_dataset(c, db_id)
    apriori_chart_ids = build_apriori_charts(c, apriori_ds_id)
    apriori_dash_id = ensure_apriori_dashboard(c, apriori_chart_ids)
    print(f"[provision] XONG. Dashboard Apriori id={apriori_dash_id}: "
          f"http://localhost:{os.environ.get('SUPERSET_PORT', '8088')}/superset/dashboard/{apriori_dash_id}/")


if __name__ == "__main__":
    main()
