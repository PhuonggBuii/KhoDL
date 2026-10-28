"""Kiem tra thuc: voi MOI chart da tao trong provision_superset.py, doc lai
params da luu (khong gia dinh), dung dung params do de goi /api/v1/chart/data
(API ma Superset Explore/Dashboard dung de ve chart thuc te), roi doi chieu
ket qua voi so lieu da biet tu psql truc tiep tren postgres-dw.

Chay: uv run scripts/superset/verify_charts.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _client import SupersetClient  # noqa: E402


def query_context_for(chart: dict) -> dict:
    params = json.loads(chart["params"])
    ds_id = int(params["datasource"].split("__")[0])
    viz_type = chart["viz_type"]

    if viz_type == "echarts_timeseries_line":
        q = {
            "columns": [params["x_axis"]],
            "metrics": params["metrics"],
            "groupby": [],
            "orderby": [],
            "time_range": "No filter",
            "granularity": params["x_axis"],
            "extras": {"time_grain_sqla": params.get("time_grain_sqla", "P1D")},
            "row_limit": params.get("row_limit", 1000),
            "is_timeseries": True,
        }
    elif viz_type == "dist_bar":
        metrics = params["metrics"]
        q = {
            "columns": params["groupby"],
            "metrics": metrics,
            "groupby": params["groupby"],
            "orderby": [[metrics[0], False]] if params.get("order_desc") else [],
            "row_limit": params.get("row_limit", 100),
        }
    elif viz_type == "pie":
        q = {
            "columns": params["groupby"],
            "metrics": [params["metric"]],
            "groupby": params["groupby"],
            "orderby": [],
            "row_limit": params.get("row_limit", 100),
        }
    elif viz_type == "table":
        if params.get("query_mode") == "aggregate":
            # Phase 8.1 - chart KPI/top-N (vd Apriori): khong co all_columns,
            # dung metrics + groupby (co the rong). Neu co timeseries_limit_metric
            # (co che sap xep top-N giong cac chart dist_bar on dinh cua Phase 6),
            # dung chinh metric do de ORDER BY.
            orderby = []
            if params.get("timeseries_limit_metric"):
                asc = not params.get("order_desc", False)
                orderby = [[params["timeseries_limit_metric"], asc]]
            q = {
                "columns": params.get("groupby", []),
                "metrics": params["metrics"],
                "groupby": params.get("groupby", []),
                "orderby": orderby,
                "row_limit": params.get("row_limit", 100),
            }
        else:
            order_by_cols = params.get("order_by_cols", [])
            orderby = [json.loads(col) for col in order_by_cols]
            q = {
                "columns": params["all_columns"],
                "metrics": [],
                "groupby": [],
                "orderby": orderby,
                "row_limit": params.get("row_limit", 100),
            }
    elif viz_type == "echarts_timeseries_scatter":
        # Phase 8.1 - "Generic X-Axis": x_axis co the la cot bat ky (khong bat
        # buoc thoi gian) - Superset GROUP BY chinh cot do, metrics la Y.
        q = {
            "columns": [params["x_axis"]],
            "metrics": params["metrics"],
            "groupby": params.get("groupby", []),
            "orderby": [],
            "row_limit": params.get("row_limit", 1000),
            "is_timeseries": False,
        }
    else:
        raise ValueError(f"Chua ho tro viz_type {viz_type} trong script kiem thu nay")

    return {
        "datasource": {"id": ds_id, "type": "table"},
        "force": True,
        "queries": [q],
        "form_data": {"viz_type": viz_type, "datasource": f"{ds_id}__table"},
        "result_type": "full",
        "result_format": "json",
    }


def main() -> None:
    c = SupersetClient()
    c.login()
    charts = c.get("/api/v1/chart/?q=(page:0,page_size:100)")["result"]

    for chart in sorted(charts, key=lambda x: x["id"]):
        detail = c.get(f"/api/v1/chart/{chart['id']}")["result"]
        qc = query_context_for(detail)
        resp = c.post("/api/v1/chart/data", qc)
        rows = resp["result"][0]["data"]
        print(f"\n=== Chart #{chart['id']} '{chart['slice_name']}' ({chart['viz_type']}) ===")
        print(f"  So dong tra ve: {len(rows)}")
        for row in rows[:5]:
            print(f"  {row}")
        if len(rows) > 5:
            print(f"  ... ({len(rows) - 5} dong con lai)")


if __name__ == "__main__":
    main()
